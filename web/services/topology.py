from database import db


def latest_topology():
    doc = db.topology.find_one(sort=[("created_at", -1)], projection={"_id": 0})
    return doc or {"nodes": [], "links": [], "meta": {"generated_at": None}}


def topology_graph():
    nodes = []
    for n in db.graph_nodes.find({}, {"_id": 1}):
        node_id = n["_id"]
        if node_id.startswith("name:"):
            device_name = node_id[5:]
            dev = db.devices.find_one(
                {"$or": [{"display_name": device_name}, {"host": ""}]},
                {"display_name": 1, "host": 1, "device_type": 1}
            )
            if dev:
                label = dev.get("display_name") or device_name
                device_type = dev.get("device_type")
            else:
                label = device_name
                device_type = None
        else:
            dev = db.devices.find_one({"host": node_id}, {"display_name": 1, "host": 1, "device_type": 1})
            if dev:
                label = dev.get("display_name") or dev.get("host") or node_id
                device_type = dev.get("device_type")
            else:
                label = node_id
                device_type = None
        nodes.append({"id": node_id, "label": label, "device_type": device_type})

    edges = []
    for e in db.graph_links.find({}, {"_id": 0, "a": 1, "b": 1, "ifA": 1, "ifB": 1}):
        edges.append({
            "source": e["a"],
            "target": e["b"],
            "ifSrc": e.get("ifA", ""),
            "ifDst": e.get("ifB", "")
        })
    return {"nodes": nodes, "links": edges}


def clear_topology():
    gn = db.graph_nodes.delete_many({}).deleted_count
    gl = db.graph_links.delete_many({}).deleted_count
    tp = db.topology.delete_many({}).deleted_count
    return {"cleared": True, "graph_nodes": gn, "graph_links": gl, "topology_docs": tp}
