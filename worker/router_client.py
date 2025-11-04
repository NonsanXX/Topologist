import time
import socket
from netmiko import ConnectHandler


def get_reachable_devices(db):
    """Return devices reachable with status ready or scanning.

    Returns:
        list: Device documents that can be used as jump hosts.
    """
    return list(
        db.devices.find(
            {
                "status": {"$in": ["ready", "scanning"]},
                "host": {"$exists": True, "$ne": ""},
            }
        )
    )


def get_directly_reachable_devices(db):
    """Return IPs that pass a TCP reachability check from this host.

    Uses cached results to avoid repeated connection attempts.

    Returns:
        set: IPs that are directly reachable.
    """
    # Try to use cached results first (with 5 minute expiry)
    cache = db.reachability_cache.find_one({"_id": "direct_reachable"})
    if cache and (time.time() - cache.get("updated_at", 0)) < 300:
        cached_ips = set(cache.get("reachable_ips", []))
        print(f"[REACHABILITY] Using cached results: {cached_ips}")
        return cached_ips

    # Test connectivity to all ready devices
    reachable_ips = set()
    devices = list(
        db.devices.find(
            {
                "status": {"$in": ["ready", "scanning"]},
                "host": {"$exists": True, "$ne": ""},
            }
        )
    )

    device_count = len(devices)
    print(
        f"[REACHABILITY] Testing {device_count} devices for direct connectivity..."
    )

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
        upsert=True,
    )

    print(f"[REACHABILITY] Found {len(reachable_ips)} directly reachable devices")
    return reachable_ips


def find_path_to_device(target_ip, reachable_devices, db):
    """Find a path to reach target_ip through intermediate jump hosts.

    Uses breadth-first search through the topology graph to find a shortest path
    while confirming starting points with direct TCP connectivity tests.

    Args:
        target_ip: IP address of target device to reach.
        reachable_devices: Devices we can potentially use as jump hosts.
        db: Database connection.

    Returns:
        list: Device IPs forming a path to the target, or None if not found.
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
    directly_reachable_ips = get_directly_reachable_devices(db)

    if not directly_reachable_ips:
        print(
            "[PATH FINDING] No directly reachable devices found via connectivity test"
        )
        return None

    print(
        "[PATH FINDING] Starting BFS from directly reachable devices: "
        f"{directly_reachable_ips}"
    )

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
                        path_str = " -> ".join(new_path)
                        print(
                            "[PATH FINDING] Found path of length "
                            f"{len(new_path)}: {path_str}"
                        )
                    break  # Found path from this starting point

                queue.append((neighbor, new_path))

    return best_path


def connect_with_jump_hosts(target_device, jump_path, db):
    """Connect to a device through a chain of SSH jump hosts.

    Args:
        target_device: Device document for the target to connect to.
        jump_path: IPs forming path [jump1, jump2, ..., target].
        db: Database connection.

    Returns:
        ConnectHandler: Connected Netmiko session, or None if connection fails.
    """
    if not jump_path or len(jump_path) < 2:
        return None

    path_display = " -> ".join(jump_path)
    print(f"[SSH CHAIN] Attempting connection via path: {path_display}")

    try:
        # Connect to the first jump host (directly reachable)
        jump_host_ip = jump_path[0]
        jump_device = db.devices.find_one({"host": jump_host_ip})

        if (
            not jump_device
            or not jump_device.get("username")
            or not jump_device.get("password")
        ):
            print(f"[SSH CHAIN] Missing credentials for jump host {jump_host_ip}")
            return None

        # Connect to first hop
        conn = ConnectHandler(
            device_type=jump_device.get("platform", "cisco_ios"),
            host=jump_host_ip,
            username=jump_device["username"],
            password=jump_device["password"],
            fast_cli=True,
        )

        print(f"[SSH CHAIN] Connected to jump host {jump_host_ip}")

        # Now SSH through each intermediate hop to reach the target
        for hop_index in range(1, len(jump_path)):
            next_ip = jump_path[hop_index]

            # Get credentials for the next hop
            if hop_index == len(jump_path) - 1:
                # This is the target device
                next_username = target_device.get("username", "admin")
                next_password = target_device.get("password", "")
            else:
                # This is an intermediate hop
                next_device = db.devices.find_one({"host": next_ip})
                if (
                    not next_device
                    or not next_device.get("username")
                    or not next_device.get("password")
                ):
                    print(
                        "[SSH CHAIN] Missing credentials for intermediate hop "
                        f"{next_ip}"
                    )
                    conn.disconnect()
                    return None
                next_username = next_device["username"]
                next_password = next_device["password"]

            # SSH to the next hop
            ssh_cmd = f"ssh -l {next_username} {next_ip}"
            prev_ip = jump_path[hop_index - 1]
            print(f"[SSH CHAIN] Hop {hop_index}: {prev_ip} -> {next_ip}")
            print(f"[SSH CHAIN] Sending: {ssh_cmd}")

            # Send SSH command and wait for output
            output = conn.send_command_timing(
                ssh_cmd, delay_factor=4, read_timeout=20
            )
            print(f"[SSH CHAIN] Initial output: {repr(output[:300])}")

            # Handle various prompts
            max_attempts = 10
            attempt = 0
            connected = False

            while attempt < max_attempts:
                attempt += 1

                # Check for password prompt first (case insensitive)
                if "password:" in output.lower():
                    print("[SSH CHAIN] Password prompt detected, sending password")
                    output = conn.send_command_timing(
                        next_password, delay_factor=3, read_timeout=15
                    )
                    print(f"[SSH CHAIN] After password: {repr(output[:300])}")
                    # Continue to check for prompt below

                # Check for yes/no prompt for SSH key
                if (
                    "(yes/no" in output.lower()
                    or "continue connecting" in output.lower()
                ):
                    print("[SSH CHAIN] SSH key prompt detected, sending 'yes'")
                    output = conn.send_command_timing(
                        "yes", delay_factor=2, read_timeout=10
                    )
                    print(f"[SSH CHAIN] After 'yes': {repr(output[:300])}")
                    # Continue to check for password below

                # Check if we got a prompt (successful connection)
                if "#" in output or ">" in output:
                    # Make sure it's actually a prompt at the end of a line
                    lines = output.strip().split("\n")
                    last_line = lines[-1] if lines else ""
                    print(f"[SSH CHAIN] Checking last line: {repr(last_line)}")

                    if last_line.strip().endswith("#") or last_line.strip().endswith(
                        ">"
                    ):
                        print(f"[SSH CHAIN] Successfully connected to {next_ip}")
                        print(f"[SSH CHAIN] Prompt: {last_line}")
                        connected = True
                        break

                # If output is empty or no clear state, wait and read more
                if not output or len(output.strip()) == 0:
                    print("[SSH CHAIN] Empty output, waiting for data...")
                    time.sleep(2)
                    output = conn.send_command_timing(
                        "", delay_factor=2, read_timeout=10
                    )
                    print(f"[SSH CHAIN] After wait: {repr(output[:300])}")
                else:
                    # Got some output but no password/prompt yet, keep waiting
                    print("[SSH CHAIN] Waiting for password prompt or device prompt...")
                    time.sleep(1)
                    output += conn.send_command_timing(
                        "", delay_factor=1, read_timeout=10
                    )
                    print(f"[SSH CHAIN] Accumulated output: {repr(output[:300])}")

            if not connected:
                print(
                    "[SSH CHAIN] Failed to connect to "
                    f"{next_ip} after {max_attempts} attempts"
                )
                print(f"[SSH CHAIN] Final output: {repr(output)}")
                conn.disconnect()
                return None

        # Successfully connected through all hops to target
        target_ip = jump_path[-1]
        path_display = " -> ".join(jump_path)
        print(
            "[SSH CHAIN] Successfully reached target "
            f"{target_ip} through path: {path_display}"
        )

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
