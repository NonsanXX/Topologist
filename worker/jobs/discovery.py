import time
from bson.objectid import ObjectId

from parsers import parse_lldp_cisco, parse_cdp_cisco
from consumer import enqueue_discovery, trigger_discover_all
from .utils import establish_connection


def build_graph(seed_ip, links, db):
    nodes = set([seed_ip])
    edges = set()
    for link in links:
        r_ip = link.get("remote_mgmt_ip")
        r_name = link.get("remote_sysname")

        if r_ip:
            device_by_ip = db.devices.find_one({"host": r_ip})
            device_by_name = db.devices.find_one(
                {"display_name": r_name, "host": {"$ne": ""}}
            )
            device_by_alt_ip = db.devices.find_one({"alternate_ips": r_ip})

            if device_by_alt_ip:
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
            continue

        nodes.add(remote_id)
        a, b = sorted([seed_ip, remote_id])
        if a == seed_ip:
            edges.add((a, b, link.get("local_if"), link.get("remote_port")))
        else:
            edges.add((a, b, link.get("remote_port"), link.get("local_if")))
    return [{"id": n} for n in sorted(nodes)], [
        {"source": a, "target": b, "ifSrc": x, "ifDst": y}
        for (a, b, x, y) in sorted(edges)
    ]


def upsert_graph(seed_ip, links, db):
    now = time.time()
    for link in links:
        r_ip = link.get("remote_mgmt_ip")
        r_name = link.get("remote_sysname")

        if r_ip:
            device_by_ip = db.devices.find_one({"host": r_ip})
            device_by_name = db.devices.find_one(
                {"display_name": r_name, "host": {"$ne": ""}}
            )
            device_by_alt_ip = db.devices.find_one({"alternate_ips": r_ip})

            if device_by_alt_ip:
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
            continue

        db.graph_nodes.update_one(
            {"_id": seed_ip},
            {
                "$set": {"id": seed_ip, "last_seen": now},
                "$setOnInsert": {"first_seen": now},
            },
            upsert=True,
        )

        db.graph_nodes.update_one(
            {"_id": remote_id},
            {
                "$set": {"id": remote_id, "last_seen": now},
                "$setOnInsert": {"first_seen": now},
            },
            upsert=True,
        )

        a, b = sorted([seed_ip, remote_id])
        if_a = link.get("local_if") if a == seed_ip else link.get("remote_port")
        if_b = link.get("remote_port") if b == remote_id else link.get("local_if")
        edge_id = f"{a}|{b}"
        db.graph_links.update_one(
            {"_id": edge_id},
            {
                "$set": {"a": a, "b": b, "ifA": if_a, "ifB": if_b, "last_seen": now},
                "$setOnInsert": {"first_seen": now},
            },
            upsert=True,
        )


def write_topology(seed_ip, nodes, edges, brief, db):
    db.topology.insert_one(
        {
            "created_at": time.time(),
            "seed": seed_ip,
            "nodes": nodes,
            "links": edges,
            "interface_brief": brief,
        }
    )


def do_discovery_job(job, db):
    oid = ObjectId(job["device_id"])
    depth = int(job.get("depth", 0))
    auto_recursive = bool(job.get("auto_recursive", False))
    max_depth = int(job.get("max_depth", 2))

    dev = db.devices.find_one({"_id": oid})
    if not dev:
        print("device missing", oid)
        return

    db.devices.update_one(
        {"_id": oid}, {"$set": {"status": "scanning", "last_seen": time.time()}}
    )

    conn = None
    used_proxy = False

    try:
        seed_ip = dev["host"]

        if not seed_ip or seed_ip.strip() == "":
            db.devices.update_one(
                {"_id": oid}, {"$set": {"status": "needs_ip", "last_seen": time.time()}}
            )
            print(f"no IP for device {dev.get('display_name', oid)}")
            return

        if not dev.get("username") or not dev.get("password"):
            db.devices.update_one({"_id": oid}, {"$set": {"status": "needs_creds"}})
            print("no creds for", seed_ip)
            return

        conn, used_proxy = establish_connection(dev, db)

        if used_proxy:
            out = conn.send_command_timing(
                "show cdp neighbors detail", delay_factor=4, read_timeout=30
            )
        else:
            out = conn.send_command("show cdp neighbors detail", expect_string=r"#")

        links = parse_cdp_cisco(out)
        used_proto = "cdp"

        if not links:
            if used_proxy:
                out = conn.send_command_timing(
                    "show lldp neighbors detail", delay_factor=4, read_timeout=30
                )
            else:
                out = conn.send_command(
                    "show lldp neighbors detail", expect_string=r"#"
                )
            links = parse_lldp_cisco(out)
            for l in links:
                l["device_type"] = None
            used_proto = "lldp_fallback"

        if used_proxy:
            brief = conn.send_command_timing(
                "show ip interface brief", delay_factor=4, read_timeout=30
            )
        else:
            brief = conn.send_command("show ip interface brief", expect_string=r"#")

        conn.disconnect()

        nodes, edges = build_graph(seed_ip, links, db)
        write_topology(seed_ip, nodes, edges, brief, db)
        upsert_graph(seed_ip, links, db)
        db.devices.update_one(
            {"_id": oid}, {"$set": {"status": "ready", "last_seen": time.time()}}
        )

        conn_method = "proxy" if used_proxy else "direct"
        print(
            f"[DISCOVERY] seed={seed_ip} method={conn_method} protocol={used_proto} neighbors={len(links)}"
        )

        default_identity = db.identities.find_one({"is_default": True})
        default_identity_id = str(default_identity["_id"]) if default_identity else None
        default_username = default_identity["username"] if default_identity else None
        default_password = default_identity["password"] if default_identity else None
        default_status = (
            "ready" if (default_username and default_password) else "needs_creds"
        )

        new_devices_added = False

        for link in links:
            r_ip = link.get("remote_mgmt_ip")
            rname = link.get("remote_sysname")
            r_type = link.get("device_type")
            print(
                f"  neighbor name={rname} ip={r_ip or '-'} type={r_type} local_if={link.get('local_if')} remote_port={link.get('remote_port')}"
            )
            if not r_ip:
                existing = db.devices.find_one({"display_name": rname, "host": ""})
                if not existing:
                    db.devices.insert_one(
                        {
                            "host": "",
                            "display_name": rname,
                            "platform": "cisco_ios",
                            "identity_id": default_identity_id,
                            "username": default_username,
                            "password": default_password,
                            "status": default_status,
                            "depth": depth + 1,
                            "parent": seed_ip,
                            "device_type": r_type if r_type != "unknown" else None,
                            "created_at": time.time(),
                            "last_seen": None,
                        }
                    )
                    new_devices_added = True
                continue

            existing_by_name = db.devices.find_one(
                {"display_name": rname, "host": {"$ne": ""}}
            )
            existing_by_ip = db.devices.find_one({"host": r_ip})

            if existing_by_name and not existing_by_ip:
                alternate_ips = existing_by_name.get("alternate_ips", [])
                interface_map = existing_by_name.get("interface_map", {})

                if r_ip not in alternate_ips and r_ip != existing_by_name["host"]:
                    alternate_ips.append(r_ip)
                    interface_map[r_ip] = link.get("remote_port", "")

                    db.devices.update_one(
                        {"_id": existing_by_name["_id"]},
                        {
                            "$set": {
                                "alternate_ips": alternate_ips,
                                "interface_map": interface_map,
                            }
                        },
                    )

                if existing_by_name.get("depth", 999) > depth + 1:
                    db.devices.update_one(
                        {"_id": existing_by_name["_id"]},
                        {"$set": {"depth": depth + 1, "parent": seed_ip}},
                    )
                continue

            if not existing_by_ip:
                interface_map = {r_ip: link.get("remote_port", "")} if r_ip else {}
                new = {
                    "host": r_ip,
                    "display_name": rname,
                    "platform": "cisco_ios",
                    "identity_id": default_identity_id,
                    "username": default_username,
                    "password": default_password,
                    "status": default_status,
                    "depth": depth + 1,
                    "parent": seed_ip,
                    "device_type": r_type if r_type != "unknown" else None,
                    "alternate_ips": [],
                    "interface_map": interface_map,
                    "created_at": time.time(),
                    "last_seen": None,
                }
                new_id = db.devices.insert_one(new).inserted_id
                new_devices_added = True
                if auto_recursive and (depth + 1) <= max_depth:
                    enqueue_discovery(str(new_id), depth + 1, auto_recursive, max_depth)
            else:
                upd = {}
                if (
                    "display_name" not in existing_by_ip
                    or not existing_by_ip["display_name"]
                ):
                    upd["display_name"] = rname
                if existing_by_ip.get("depth", 999) > depth + 1:
                    upd["depth"] = depth + 1
                    upd["parent"] = seed_ip
                if (
                    (not existing_by_ip.get("device_type"))
                    and r_type
                    and r_type != "unknown"
                ):
                    upd["device_type"] = r_type
                interface_map = existing_by_ip.get("interface_map", {})
                if r_ip and link.get("remote_port"):
                    interface_map[r_ip] = link.get("remote_port")
                    upd["interface_map"] = interface_map
                if upd:
                    db.devices.update_one({"_id": existing_by_ip["_id"]}, {"$set": upd})

        if new_devices_added:
            print("[DISCOVERY] New devices added, triggering cascading discovery")
            trigger_discover_all(db)

    except Exception as e:
        print("discovery error:", e)
        db.devices.update_one(
            {"_id": oid},
            {"$set": {"status": "error", "error": str(e), "last_seen": time.time()}},
        )
        if conn:
            try:
                conn.disconnect()
            except Exception:
                pass
