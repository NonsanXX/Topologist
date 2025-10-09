import os, json, time, pika, pymongo
from bson.objectid import ObjectId
from netmiko import ConnectHandler
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

def build_graph(seed_ip, links):
    """links: list of dicts from parser (local_if, remote_sysname, remote_port, remote_mgmt_ip)
    
    Supports both IP-based and name-based node identifiers.
    - If remote device has mgmt IP: use IP as node ID
    - If no IP: use 'name:<remote_sysname>' as node ID
    """
    nodes = set([seed_ip])
    edges = set()
    for link in links:
        r_ip = link.get("remote_mgmt_ip")
        r_name = link.get("remote_sysname")
        
        # Create node ID: prefer IP, fallback to name-based identifier
        if r_ip:
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
    
    Supports both IP-based and name-based node identifiers.
    """
    now = time.time()
    for link in links:
        r_ip = link.get("remote_mgmt_ip")
        r_name = link.get("remote_sysname")
        
        # Create remote node ID: prefer IP, fallback to name-based identifier
        if r_ip:
            remote_id = r_ip
        elif r_name:
            remote_id = f"name:{r_name}"
        else:
            # Skip if neither IP nor name available
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

def do_discovery_job(job):
    oid = ObjectId(job["device_id"])
    depth = int(job.get("depth",0))
    auto_recursive = bool(job.get("auto_recursive", False))
    max_depth = int(job.get("max_depth", 2))

    dev = db.devices.find_one({"_id": oid})
    if not dev: 
        print("device missing", oid); return

    db.devices.update_one({"_id": oid},{"$set":{"status":"scanning","last_seen":time.time()}})
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

        conn = ConnectHandler(
            device_type=dev.get("platform","cisco_ios"),
            host=seed_ip, username=dev["username"], password=dev["password"], fast_cli=True
        )

        # ดึง CDP ก่อนเพื่อใช้ capability เป็นหลัก (ตามคำขอ) แล้วค่อย fallback LLDP ถ้า CDP ว่าง
        out = conn.send_command("show cdp neighbors detail", expect_string=r"#")
        links = parse_cdp_cisco(out)
        used_proto = "cdp"
        if not links:
            out = conn.send_command("show lldp neighbors detail", expect_string=r"#")
            links = parse_lldp_cisco(out)
            # ล้าง device_type จาก LLDP เพื่อไม่ให้ override (CDP-only classification)
            for l in links:
                l["device_type"] = None
            used_proto = "lldp_fallback"

        # สถานะพอร์ต
        brief = conn.send_command("show ip interface brief", expect_string=r"#")
        conn.disconnect()

        # topology ใช้ IP เท่านั้น
        nodes, edges = build_graph(seed_ip, links)
        write_topology(seed_ip, nodes, edges, brief)
        upsert_graph(seed_ip, links)     # ← เพิ่มบรรทัดนี้
        db.devices.update_one({"_id": oid},{"$set":{"status":"ready","last_seen":time.time()}})

        print(f"[DISCOVERY] seed={seed_ip} protocol={used_proto} neighbors={len(links)}")
        
        # Get default identity if exists
        default_identity = db.identities.find_one({"is_default": True})
        default_identity_id = str(default_identity["_id"]) if default_identity else None
        default_username = default_identity["username"] if default_identity else None
        default_password = default_identity["password"] if default_identity else None
        default_status = "ready" if (default_username and default_password) else "needs_creds"
        
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
                continue

            # มี IP → ใช้ IP เป็น host
            existing = db.devices.find_one({"host": r_ip})
            if not existing:
                new = {
                    "host": r_ip, "display_name": rname,
                    "platform": "cisco_ios",
                    "identity_id": default_identity_id,
                    "username": default_username,
                    "password": default_password,
                    "status": default_status,
                    "depth": depth+1, "parent": seed_ip,
                    "device_type": r_type if r_type != 'unknown' else None,
                    "created_at": time.time(), "last_seen": None
                }
                new_id = db.devices.insert_one(new).inserted_id
                if auto_recursive and (depth+1) <= max_depth:
                    enqueue_discovery(str(new_id), depth+1, auto_recursive, max_depth)
            else:
                # อัปเดตชื่อ / depth / parent ถ้าจำเป็น
                upd = {}
                if "display_name" not in existing or not existing["display_name"]:
                    upd["display_name"] = rname
                if existing.get("depth", 999) > depth+1:
                    upd["depth"] = depth+1; upd["parent"] = seed_ip
                # ถ้าไม่มี device_type เดิม และเราตรวจพบ
                if (not existing.get("device_type")) and r_type and r_type != 'unknown':
                    upd["device_type"] = r_type
                if upd:
                    db.devices.update_one({"_id": existing["_id"]}, {"$set": upd})

    except Exception as e:
        print("discovery error:", e)
        db.devices.update_one({"_id": oid},{"$set":{"status":"error","error":str(e),"last_seen":time.time()}})

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
