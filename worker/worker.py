import os, json, time, pika, pymongo
from bson.objectid import ObjectId
from netmiko import ConnectHandler
from netmiko.exceptions import NetmikoTimeoutException, NetmikoAuthenticationException
from parsers import parse_lldp_cisco, parse_cdp_cisco

MONGO_URI   = os.getenv("MONGO_URI","mongodb://mongo:27017")
DB_NAME     = os.getenv("DB_NAME","topologist")
RABBIT_HOST = os.getenv("RABBIT_HOST","rabbitmq")
db = pymongo.MongoClient(MONGO_URI)[DB_NAME]

def connect_to_rabbitmq(max_retries=10, initial_delay=1):
    """Connect to RabbitMQ with retry logic and exponential backoff.
    
    Args:
        max_retries: Maximum number of connection attempts (0 = infinite)
        initial_delay: Initial delay in seconds between retries
    
    Returns:
        pika.BlockingConnection: Connected RabbitMQ connection
    """
    delay = initial_delay
    attempt = 0
    
    while max_retries == 0 or attempt < max_retries:
        try:
            attempt += 1
            print(f"[RabbitMQ] Connection attempt {attempt}...")
            conn = pika.BlockingConnection(pika.ConnectionParameters(
                host=RABBIT_HOST,
                heartbeat=600,
                blocked_connection_timeout=300
            ))
            print(f"[RabbitMQ] Connected successfully!")
            return conn
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.ConnectionClosedByBroker) as e:
            if max_retries > 0 and attempt >= max_retries:
                print(f"[RabbitMQ] Failed after {max_retries} attempts")
                raise
            print(f"[RabbitMQ] Connection failed: {e}")
            print(f"[RabbitMQ] Retrying in {delay} seconds...")
            time.sleep(delay)
            delay = min(delay * 2, 30)  # Exponential backoff, max 30 seconds

def get_reachable_devices():
    """Get list of devices that were successfully connected to (status=ready or scanning).
    
    Returns:
        list: List of device documents that can potentially be used as jump hosts
    """
    return list(db.devices.find({
        "status": {"$in": ["ready", "scanning"]},
        "host": {"$exists": True, "$ne": ""}
    }))

def get_directly_reachable_devices():
    """Get list of devices that can be reached directly from this PC.
    
    Tests actual TCP connectivity to determine reachability instead of relying on depth.
    Uses cached results to avoid repeated connection attempts.
    
    Returns:
        set: Set of IPs that are directly reachable
    """
    # Try to use cached results first (with 5 minute expiry)
    cache = db.reachability_cache.find_one({"_id": "direct_reachable"})
    if cache and (time.time() - cache.get("updated_at", 0)) < 300:
        cached_ips = set(cache.get("reachable_ips", []))
        print(f"[REACHABILITY] Using cached results: {cached_ips}")
        return cached_ips
    
    # Test connectivity to all ready devices
    reachable_ips = set()
    devices = list(db.devices.find({
        "status": {"$in": ["ready", "scanning"]},
        "host": {"$exists": True, "$ne": ""}
    }))
    
    print(f"[REACHABILITY] Testing {len(devices)} devices for direct connectivity...")
    
    import socket
    for dev in devices:
        host = dev.get("host")
        if not host:
            continue
            
        # Quick TCP connection test (don't do full SSH handshake)
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)  # 2 second timeout
            result = sock.connect_ex((host, 22))
            sock.close()
            
            if result == 0:
                reachable_ips.add(host)
                print(f"[REACHABILITY] ✓ {host} is directly reachable")
            else:
                print(f"[REACHABILITY] ✗ {host} not reachable (code {result})")
        except Exception as e:
            print(f"[REACHABILITY] ✗ {host} test failed: {e}")
    
    # Cache the results
    db.reachability_cache.update_one(
        {"_id": "direct_reachable"},
        {"$set": {"reachable_ips": list(reachable_ips), "updated_at": time.time()}},
        upsert=True
    )
    
    print(f"[REACHABILITY] Found {len(reachable_ips)} directly reachable devices")
    return reachable_ips

def find_path_to_device(target_ip, reachable_devices):
    """Find a path to reach target_ip through intermediate jump hosts.
    
    Uses breadth-first search through the topology graph to find shortest path.
    Tests actual TCP connectivity to determine which devices can be used as starting points.
    
    Args:
        target_ip: IP address of target device to reach
        reachable_devices: List of devices that we can potentially use as jump hosts
        
    Returns:
        list: List of device IPs forming a path from a directly reachable device to target,
              or None if no path found
    """
    if not reachable_devices:
        return None
    
    # Build adjacency list from graph_links
    graph = {}
    links = db.graph_links.find({})
    for link in links:
        a, b = link["a"], link["b"]
        if a not in graph:
            graph[a] = []
        if b not in graph:
            graph[b] = []
        graph[a].append(b)
        graph[b].append(a)
    
    # Get devices that are actually directly reachable (tested via TCP)
    directly_reachable_ips = get_directly_reachable_devices()
    
    if not directly_reachable_ips:
        print("[PATH FINDING] No directly reachable devices found via connectivity test")
        return None
    
    print(f"[PATH FINDING] Starting BFS from directly reachable devices: {directly_reachable_ips}")
    
    # Try to find shortest path from any directly reachable device
    best_path = None
    
    for start_ip in sorted(directly_reachable_ips):
        if start_ip == target_ip:
            return [target_ip]  # Already directly reachable
        
        # BFS from this starting point
        queue = [(start_ip, [start_ip])]
        visited = {start_ip}
        
        while queue:
            current, path = queue.pop(0)
            
            if current not in graph:
                continue
                
            for neighbor in graph[current]:
                if neighbor in visited:
                    continue
                    
                visited.add(neighbor)
                new_path = path + [neighbor]
                
                if neighbor == target_ip:
                    # Found a path! Check if it's better than current best
                    if best_path is None or len(new_path) < len(best_path):
                        best_path = new_path
                        print(f"[PATH FINDING] Found path of length {len(new_path)}: {' -> '.join(new_path)}")
                    break  # Found path from this starting point
                    
                queue.append((neighbor, new_path))
    
    return best_path

def connect_with_jump_hosts(target_device, jump_path):
    """Connect to a device through a chain of SSH jump hosts.
    
    Args:
        target_device: Device document for the target to connect to
        jump_path: List of IP addresses forming path [jump1, jump2, ..., target]
        
    Returns:
        ConnectHandler: Connected Netmiko session, or None if connection fails
    """
    if not jump_path or len(jump_path) < 2:
        return None
    
    print(f"[SSH CHAIN] Attempting connection via path: {' -> '.join(jump_path)}")
    
    try:
        # Connect to the first jump host (directly reachable)
        jump_host_ip = jump_path[0]
        jump_device = db.devices.find_one({"host": jump_host_ip})
        
        if not jump_device or not jump_device.get("username") or not jump_device.get("password"):
            print(f"[SSH CHAIN] Missing credentials for jump host {jump_host_ip}")
            return None
        
        # Connect to first hop
        conn = ConnectHandler(
            device_type=jump_device.get("platform", "cisco_ios"),
            host=jump_host_ip,
            username=jump_device["username"],
            password=jump_device["password"],
            fast_cli=True
        )
        
        print(f"[SSH CHAIN] Connected to jump host {jump_host_ip}")
        
        # Now SSH through each intermediate hop to reach the target
        for hop_index in range(1, len(jump_path)):
            next_ip = jump_path[hop_index]
            
            # Get credentials for the next hop
            if hop_index == len(jump_path) - 1:
                # This is the target device
                next_username = target_device.get('username', 'admin')
                next_password = target_device.get('password', '')
            else:
                # This is an intermediate hop
                next_device = db.devices.find_one({"host": next_ip})
                if not next_device or not next_device.get("username") or not next_device.get("password"):
                    print(f"[SSH CHAIN] Missing credentials for intermediate hop {next_ip}")
                    conn.disconnect()
                    return None
                next_username = next_device["username"]
                next_password = next_device["password"]
            
            # SSH to the next hop
            ssh_cmd = f"ssh -l {next_username} {next_ip}"
            print(f"[SSH CHAIN] Hop {hop_index}: {jump_path[hop_index-1]} -> {next_ip}")
            print(f"[SSH CHAIN] Sending: {ssh_cmd}")
            
            # Send SSH command and wait for output
            output = conn.send_command_timing(ssh_cmd, delay_factor=4, read_timeout=20)
            print(f"[SSH CHAIN] Initial output: {repr(output[:300])}")
            
            # Handle various prompts
            max_attempts = 10
            attempt = 0
            connected = False
            
            while attempt < max_attempts:
                attempt += 1
                
                # Check for password prompt (case insensitive) - check this FIRST before anything else
                if "password:" in output.lower():
                    print(f"[SSH CHAIN] Password prompt detected, sending password")
                    output = conn.send_command_timing(next_password, delay_factor=3, read_timeout=15)
                    print(f"[SSH CHAIN] After password: {repr(output[:300])}")
                    # Continue to check for prompt below
                
                # Check for yes/no prompt for SSH key
                if "(yes/no" in output.lower() or "continue connecting" in output.lower():
                    print(f"[SSH CHAIN] SSH key prompt detected, sending 'yes'")
                    output = conn.send_command_timing("yes", delay_factor=2, read_timeout=10)
                    print(f"[SSH CHAIN] After 'yes': {repr(output[:300])}")
                    # Continue to check for password below
                
                # Check if we got a prompt (successful connection)
                if "#" in output or ">" in output:
                    # Make sure it's actually a prompt at the end of a line
                    lines = output.strip().split('\n')
                    last_line = lines[-1] if lines else ""
                    print(f"[SSH CHAIN] Checking last line: {repr(last_line)}")
                    
                    if last_line.strip().endswith('#') or last_line.strip().endswith('>'):
                        print(f"[SSH CHAIN] Successfully connected to {next_ip}")
                        print(f"[SSH CHAIN] Prompt: {last_line}")
                        connected = True
                        break
                
                # If output is empty or no clear state, wait and read more
                if not output or len(output.strip()) == 0:
                    print(f"[SSH CHAIN] Empty output, waiting for data...")
                    time.sleep(2)
                    output = conn.send_command_timing("", delay_factor=2, read_timeout=10)
                    print(f"[SSH CHAIN] After wait: {repr(output[:300])}")
                else:
                    # Got some output but no password/prompt yet, keep waiting
                    print(f"[SSH CHAIN] Waiting for password prompt or device prompt...")
                    time.sleep(1)
                    output += conn.send_command_timing("", delay_factor=1, read_timeout=10)
                    print(f"[SSH CHAIN] Accumulated output: {repr(output[:300])}")
            
            if not connected:
                print(f"[SSH CHAIN] Failed to connect to {next_ip} after {max_attempts} attempts")
                print(f"[SSH CHAIN] Final output: {repr(output)}")
                conn.disconnect()
                return None
        
        # Successfully connected through all hops to target
        target_ip = jump_path[-1]
        print(f"[SSH CHAIN] Successfully reached target {target_ip} through path: {' -> '.join(jump_path)}")
        
        # Mark this as a proxy connection
        conn._proxy_mode = True
        conn._target_ip = target_ip
        conn._jump_path = jump_path
        
        return conn
            
    except Exception as e:
        print(f"[SSH CHAIN] Error connecting through jump hosts: {e}")
        import traceback
        traceback.print_exc()
        return None

def connect_to_rabbitmq(max_retries=10, initial_delay=1):
    """Connect to RabbitMQ with retry logic and exponential backoff.
    
    Args:
        max_retries: Maximum number of connection attempts (0 = infinite)
        initial_delay: Initial delay in seconds between retries
    
    Returns:
        pika.BlockingConnection: Connected RabbitMQ connection
    """
    delay = initial_delay
    attempt = 0
    
    while max_retries == 0 or attempt < max_retries:
        try:
            attempt += 1
            print(f"[RabbitMQ] Connection attempt {attempt}...")
            conn = pika.BlockingConnection(pika.ConnectionParameters(
                host=RABBIT_HOST,
                heartbeat=600,
                blocked_connection_timeout=300
            ))
            print(f"[RabbitMQ] Connected successfully!")
            return conn
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.ConnectionClosedByBroker) as e:
            if max_retries > 0 and attempt >= max_retries:
                print(f"[RabbitMQ] Failed after {max_retries} attempts")
                raise
            print(f"[RabbitMQ] Connection failed: {e}")
            print(f"[RabbitMQ] Retrying in {delay} seconds...")
            time.sleep(delay)
            delay = min(delay * 2, 30)  # Exponential backoff, max 30 seconds

def build_graph(seed_ip, links):
    """links: list of dicts from parser (local_if, remote_sysname, remote_port, remote_mgmt_ip)
    
    Uses the primary IP from devices collection to consolidate duplicate devices.
    - If remote device has mgmt IP: look up primary IP in devices collection
    - If no IP: use 'name:<remote_sysname>' as node ID
    """
    nodes = set([seed_ip])
    edges = set()
    for link in links:
        r_ip = link.get("remote_mgmt_ip")
        r_name = link.get("remote_sysname")
        
        # Create node ID: prefer IP, fallback to name-based identifier
        if r_ip:
            # Check if this IP belongs to a device with a different primary IP
            # (to handle devices with multiple interfaces)
            device_by_ip = db.devices.find_one({"host": r_ip})
            device_by_name = db.devices.find_one({"display_name": r_name, "host": {"$ne": ""}})
            device_by_alt_ip = db.devices.find_one({"alternate_ips": r_ip})
            
            # Determine the canonical/primary IP for this device
            if device_by_alt_ip:
                # This IP is an alternate IP, use the primary host instead
                remote_id = device_by_alt_ip["host"]
                print(f"  [GRAPH] Using primary IP {remote_id} instead of alternate IP {r_ip} for {r_name}")
            elif device_by_ip:
                remote_id = device_by_ip["host"]
            elif device_by_name:
                remote_id = device_by_name["host"]
            else:
                remote_id = r_ip
        elif r_name:
            remote_id = f"name:{r_name}"
        else:
            # Skip if neither IP nor name available
            continue
        
        nodes.add(remote_id)
        a, b = sorted([seed_ip, remote_id])
        if a == seed_ip:
            edges.add((a, b, link.get("local_if"), link.get("remote_port")))
        else:
            edges.add((a, b, link.get("remote_port"), link.get("local_if")))
    return [{"id": n} for n in sorted(nodes)], [
        {"source": a, "target": b, "ifSrc": x, "ifDst": y} for (a, b, x, y) in sorted(edges)
    ]

def upsert_graph(seed_ip, links):
    """Upsert nodes and edges to graph_nodes and graph_links collections.
    
    Uses the primary IP from devices collection to consolidate duplicate devices.
    """
    now = time.time()
    for link in links:
        r_ip = link.get("remote_mgmt_ip")
        r_name = link.get("remote_sysname")
        
        # Create remote node ID: check for primary IP to consolidate duplicates
        if r_ip:
            # Check if this IP belongs to a device with a different primary IP
            device_by_ip = db.devices.find_one({"host": r_ip})
            device_by_name = db.devices.find_one({"display_name": r_name, "host": {"$ne": ""}})
            device_by_alt_ip = db.devices.find_one({"alternate_ips": r_ip})
            
            # Determine the canonical/primary IP for this device
            if device_by_alt_ip:
                # This IP is an alternate IP, use the primary host instead
                remote_id = device_by_alt_ip["host"]
            elif device_by_ip:
                remote_id = device_by_ip["host"]
            elif device_by_name:
                remote_id = device_by_name["host"]
            else:
                remote_id = r_ip
        elif r_name:
            remote_id = f"name:{r_name}"
        else:
            # Skip if neither IP nor name available
            continue
            continue
        
        # Upsert seed node (always IP-based)
        db.graph_nodes.update_one(
            {"_id": seed_ip},
            {"$set": {"id": seed_ip, "last_seen": now}, "$setOnInsert": {"first_seen": now}},
            upsert=True
        )
        
        # Upsert remote node (IP or name-based)
        db.graph_nodes.update_one(
            {"_id": remote_id},
            {"$set": {"id": remote_id, "last_seen": now}, "$setOnInsert": {"first_seen": now}},
            upsert=True
        )
        
        # Create edge
        a, b = sorted([seed_ip, remote_id])
        if_a = link.get("local_if") if a == seed_ip else link.get("remote_port")
        if_b = link.get("remote_port") if b == remote_id else link.get("local_if")
        edge_id = f"{a}|{b}"
        db.graph_links.update_one(
            {"_id": edge_id},
            {"$set": {"a": a, "b": b, "ifA": if_a, "ifB": if_b, "last_seen": now}, "$setOnInsert": {"first_seen": now}},
            upsert=True
        )


def write_topology(seed_ip, nodes, edges, brief):
    db.topology.insert_one({
        "created_at": time.time(),
        "seed": seed_ip,
        "nodes": nodes,
        "links": edges,
        "interface_brief": brief
    })

def enqueue_discovery(device_id, depth, auto_recursive, max_depth):
    conn = connect_to_rabbitmq(max_retries=3, initial_delay=1)
    ch = conn.channel(); ch.queue_declare(queue="discovery", durable=True)
    job = {"type":"discovery","device_id":device_id,"depth":depth,
           "auto_recursive":auto_recursive,"max_depth":max_depth}
    ch.basic_publish(exchange="", routing_key="discovery", body=json.dumps(job))
    conn.close()

def trigger_discover_all():
    """Trigger discover_all by enqueueing discovery jobs for all ready/error devices."""
    try:
        devices = list(db.devices.find({"status": {"$in": ["ready", "error"]}}, {"_id": 1, "depth": 1}))
        if not devices:
            print("[AUTO DISCOVER] No devices to discover")
            return
        
        conn = connect_to_rabbitmq(max_retries=3, initial_delay=1)
        ch = conn.channel()
        ch.queue_declare(queue="discovery", durable=True)
        
        count = 0
        for d in devices:
            job = {
                "type": "discovery",
                "device_id": str(d["_id"]),
                "depth": int(d.get("depth", 0)),
                "auto_recursive": False,
                "max_depth": 3
            }
            ch.basic_publish(exchange="", routing_key="discovery", body=json.dumps(job))
            count += 1
        
        conn.close()
        print(f"[AUTO DISCOVER] Queued {count} devices for discovery")
    except Exception as e:
        print(f"[AUTO DISCOVER] Error: {e}")

def do_discovery_job(job):
    oid = ObjectId(job["device_id"])
    depth = int(job.get("depth",0))
    auto_recursive = bool(job.get("auto_recursive", False))
    max_depth = int(job.get("max_depth", 2))

    dev = db.devices.find_one({"_id": oid})
    if not dev: 
        print("device missing", oid); return

    db.devices.update_one({"_id": oid},{"$set":{"status":"scanning","last_seen":time.time()}})
    
    conn = None
    used_proxy = False
    
    try:
        # ต้องมี IP + creds
        seed_ip = dev["host"]
        
        # Skip devices without IP address
        if not seed_ip or seed_ip.strip() == "":
            db.devices.update_one({"_id": oid},{"$set":{"status":"needs_ip","last_seen":time.time()}})
            print(f"no IP for device {dev.get('display_name', oid)}"); 
            return
        
        if not dev.get("username") or not dev.get("password"):
            db.devices.update_one({"_id": oid},{"$set":{"status":"needs_creds"}})
            print("no creds for", seed_ip); return

        # Try direct connection first
        try:
            print(f"[DISCOVERY] Attempting direct connection to {seed_ip}")
            conn = ConnectHandler(
                device_type=dev.get("platform","cisco_ios"),
                host=seed_ip, 
                username=dev["username"], 
                password=dev["password"], 
                fast_cli=True,
                timeout=10,
                conn_timeout=10
            )
            print(f"[DISCOVERY] Direct connection successful to {seed_ip}")
        except Exception as direct_err:
            print(f"[DISCOVERY] Direct connection failed to {seed_ip}: {direct_err}")
            
            # Try to find a path through jump hosts
            print(f"[DISCOVERY] Attempting SSH chain discovery for {seed_ip}")
            reachable = get_reachable_devices()
            path = find_path_to_device(seed_ip, reachable)
            
            if path and len(path) >= 2:
                print(f"[DISCOVERY] Found path to {seed_ip}: {' -> '.join(path)}")
                conn = connect_with_jump_hosts(dev, path)
                if conn:
                    used_proxy = True
                    print(f"[DISCOVERY] SSH chain connection successful to {seed_ip}")
                else:
                    raise Exception(f"SSH chain connection failed through path: {' -> '.join(path)}")
            else:
                print(f"[DISCOVERY] No path found to {seed_ip}")
                raise Exception(f"Device unreachable: {direct_err}")

        if not conn:
            raise Exception("Failed to establish connection")

        # ดึง CDP ก่อนเพื่อใช้ capability เป็นหลัก (ตามคำขอ) แล้วค่อย fallback LLDP ถ้า CDP ว่าง
        if used_proxy:
            # In proxy mode, we're tunneled through SSH, use send_command_timing instead
            out = conn.send_command_timing("show cdp neighbors detail", delay_factor=4, read_timeout=30)
        else:
            out = conn.send_command("show cdp neighbors detail", expect_string=r"#")
            
        links = parse_cdp_cisco(out)
        used_proto = "cdp"
        
        if not links:
            if used_proxy:
                out = conn.send_command_timing("show lldp neighbors detail", delay_factor=4, read_timeout=30)
            else:
                out = conn.send_command("show lldp neighbors detail", expect_string=r"#")
            links = parse_lldp_cisco(out)
            # ล้าง device_type จาก LLDP เพื่อไม่ให้ override (CDP-only classification)
            for l in links:
                l["device_type"] = None
            used_proto = "lldp_fallback"

        # สถานะพอร์ต
        if used_proxy:
            brief = conn.send_command_timing("show ip interface brief", delay_factor=4, read_timeout=30)
        else:
            brief = conn.send_command("show ip interface brief", expect_string=r"#")
            
        conn.disconnect()

        # topology ใช้ IP เท่านั้น
        nodes, edges = build_graph(seed_ip, links)
        write_topology(seed_ip, nodes, edges, brief)
        upsert_graph(seed_ip, links)     # ← เพิ่มบรรทัดนี้
        db.devices.update_one({"_id": oid},{"$set":{"status":"ready","last_seen":time.time()}})

        conn_method = "proxy" if used_proxy else "direct"
        print(f"[DISCOVERY] seed={seed_ip} method={conn_method} protocol={used_proto} neighbors={len(links)}")
        
        # Get default identity if exists
        default_identity = db.identities.find_one({"is_default": True})
        default_identity_id = str(default_identity["_id"]) if default_identity else None
        default_username = default_identity["username"] if default_identity else None
        default_password = default_identity["password"] if default_identity else None
        default_status = "ready" if (default_username and default_password) else "needs_creds"
        
        # Track if we added any new devices
        new_devices_added = False
        
        # เพื่อนบ้าน → เพิ่มลง devices ด้วย host = management IP (ถ้ามี)
        for link in links:
            r_ip = link.get("remote_mgmt_ip")
            rname = link.get("remote_sysname")
            r_type = link.get("device_type")
            print(f"  neighbor name={rname} ip={r_ip or '-'} type={r_type} local_if={link.get('local_if')} remote_port={link.get('remote_port')}")
            if not r_ip:
                # ไม่มี IP → สร้าง entry ไว้ก่อน เพื่อให้ user ใส่ IP/creds เอง
                # ใช้ชื่อ rname เก็บใน field display_name เพื่อโชว์ แต่ host เว้นว่าง
                existing = db.devices.find_one({"display_name": rname, "host": ""})
                if not existing:
                    db.devices.insert_one({
                        "host": "",               # ยังไม่รู้ IP
                        "display_name": rname,    # ไว้โชว์ชื่อ
                        "platform": "cisco_ios",
                        "identity_id": default_identity_id,
                        "username": default_username,
                        "password": default_password,
                        "status": default_status,
                        "depth": depth+1, "parent": seed_ip,
                        "device_type": r_type if r_type != 'unknown' else None,
                        "created_at": time.time(), "last_seen": None
                    })
                    new_devices_added = True
                continue

            # มี IP → check for duplicate by display_name first (same device, different IP)
            existing_by_name = db.devices.find_one({"display_name": rname, "host": {"$ne": ""}})
            existing_by_ip = db.devices.find_one({"host": r_ip})
            
            if existing_by_name and not existing_by_ip:
                # Same device name but different IP - this is the same device with multiple interfaces
                print(f"  [DEDUP] Found existing device {rname} with IP {existing_by_name['host']}, adding {r_ip} as alternate IP")
                
                # Store alternate IPs with their interfaces in the device document
                alternate_ips = existing_by_name.get("alternate_ips", [])
                interface_map = existing_by_name.get("interface_map", {})
                
                if r_ip not in alternate_ips and r_ip != existing_by_name["host"]:
                    alternate_ips.append(r_ip)
                    # Store the interface for this IP (remote_port is the interface on the remote device)
                    interface_map[r_ip] = link.get("remote_port", "")
                    
                    db.devices.update_one(
                        {"_id": existing_by_name["_id"]},
                        {"$set": {"alternate_ips": alternate_ips, "interface_map": interface_map}}
                    )
                
                # Update depth if this path is shorter
                if existing_by_name.get("depth", 999) > depth+1:
                    db.devices.update_one(
                        {"_id": existing_by_name["_id"]},
                        {"$set": {"depth": depth+1, "parent": seed_ip}}
                    )
                continue
            
            if not existing_by_ip:
                # Completely new device
                interface_map = {r_ip: link.get("remote_port", "")} if r_ip else {}
                new = {
                    "host": r_ip, "display_name": rname,
                    "platform": "cisco_ios",
                    "identity_id": default_identity_id,
                    "username": default_username,
                    "password": default_password,
                    "status": default_status,
                    "depth": depth+1, "parent": seed_ip,
                    "device_type": r_type if r_type != 'unknown' else None,
                    "alternate_ips": [],
                    "interface_map": interface_map,
                    "created_at": time.time(), "last_seen": None
                }
                new_id = db.devices.insert_one(new).inserted_id
                new_devices_added = True
                if auto_recursive and (depth+1) <= max_depth:
                    enqueue_discovery(str(new_id), depth+1, auto_recursive, max_depth)
            else:
                # Device with this IP already exists - just update metadata and interface
                upd = {}
                if "display_name" not in existing_by_ip or not existing_by_ip["display_name"]:
                    upd["display_name"] = rname
                if existing_by_ip.get("depth", 999) > depth+1:
                    upd["depth"] = depth+1; upd["parent"] = seed_ip
                # ถ้าไม่มี device_type เดิม และเราตรวจพบ
                if (not existing_by_ip.get("device_type")) and r_type and r_type != 'unknown':
                    upd["device_type"] = r_type
                # Update interface map for primary IP
                interface_map = existing_by_ip.get("interface_map", {})
                if r_ip and link.get("remote_port"):
                    interface_map[r_ip] = link.get("remote_port")
                    upd["interface_map"] = interface_map
                if upd:
                    db.devices.update_one({"_id": existing_by_ip["_id"]}, {"$set": upd})
        
        # If we added new devices, trigger discover_all to cascade discovery
        if new_devices_added:
            print(f"[DISCOVERY] New devices added, triggering cascading discovery")
            trigger_discover_all()

    except Exception as e:
        print("discovery error:", e)
        db.devices.update_one({"_id": oid},{"$set":{"status":"error","error":str(e),"last_seen":time.time()}})
        if conn:
            try:
                conn.disconnect()
            except:
                pass

def consume():
    """Main consumer loop with automatic reconnection on connection failure."""
    while True:
        try:
            # Connect with retry logic
            conn = connect_to_rabbitmq(max_retries=0, initial_delay=2)  # Infinite retries
            ch = conn.channel()
            ch.queue_declare(queue="discovery", durable=True)
            
            def cb(ch, method, props, body):
                job = json.loads(body)
                if job.get("type")=="discovery":
                    do_discovery_job(job)
                ch.basic_ack(delivery_tag=method.delivery_tag)
            
            ch.basic_consume(queue="discovery", on_message_callback=cb)
            print(" [*] worker waiting ...")
            ch.start_consuming()
            
        except (pika.exceptions.AMQPConnectionError, 
                pika.exceptions.ConnectionClosedByBroker,
                pika.exceptions.StreamLostError) as e:
            print(f"[RabbitMQ] Connection lost: {e}")
            print("[RabbitMQ] Reconnecting in 5 seconds...")
            time.sleep(5)
        except KeyboardInterrupt:
            print("\n[Worker] Shutting down gracefully...")
            break
        except Exception as e:
            print(f"[Worker] Unexpected error: {e}")
            print("[Worker] Restarting in 10 seconds...")
            time.sleep(10)

if __name__=="__main__":
    consume()
