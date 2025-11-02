from netmiko import ConnectHandler

from router_client import (
    get_reachable_devices,
    find_path_to_device,
    connect_with_jump_hosts,
)


def establish_connection(dev, db):
    seed_ip = dev.get("host")
    if not seed_ip or seed_ip.strip() == "":
        raise ValueError("Device missing IP address")

    username = dev.get("username")
    password = dev.get("password")
    if not username or not password:
        raise ValueError("Device missing credentials")

    platform = dev.get("platform", "cisco_ios")

    try:
        print(f"[SSH] Attempting direct connection to {seed_ip}")
        conn = ConnectHandler(
            device_type=platform,
            host=seed_ip,
            username=username,
            password=password,
            fast_cli=True,
            timeout=10,
            conn_timeout=10,
        )
        print(f"[SSH] Direct connection successful to {seed_ip}")
        return conn, False
    except Exception as direct_err:
        print(f"[SSH] Direct connection failed to {seed_ip}: {direct_err}")
        print(f"[SSH] Attempting SSH chain discovery for {seed_ip}")
        reachable = get_reachable_devices(db)
        path = find_path_to_device(seed_ip, reachable, db)

        if path and len(path) >= 2:
            print(f"[SSH] Found path to {seed_ip}: {' -> '.join(path)}")
            conn = connect_with_jump_hosts(dev, path, db)
            if conn:
                print(f"[SSH] SSH chain connection successful to {seed_ip}")
                return conn, True
            raise Exception(f"SSH chain connection failed through path: {' -> '.join(path)}")

        raise Exception(f"Device unreachable: {direct_err}")


def run_command_list(conn, commands, use_timing=False):
    outputs = []
    for cmd in commands:
        try:
            if use_timing:
                out = conn.send_command_timing(cmd, delay_factor=4, read_timeout=60)
            else:
                out = conn.send_command(cmd, expect_string=r"#", delay_factor=2, read_timeout=60)
        except Exception as exc:
            out = f"ERROR executing '{cmd}': {exc}"
        outputs.append((cmd, out))
    return outputs
