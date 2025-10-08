import os, json, time
from fastapi import FastAPI, Body, HTTPException
from pymongo import MongoClient
from bson.objectid import ObjectId
import pika

MONGO_URI=os.getenv("MONGO_URI","mongodb://mongo:27017")
DB_NAME=os.getenv("DB_NAME","topologist")
db=MongoClient(MONGO_URI)[DB_NAME]

def enqueue(queue, payload):
    conn=pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))
    ch=conn.channel()
    ch.queue_declare(queue=queue, durable=True)
    ch.basic_publish(exchange="", routing_key=queue, body=json.dumps(payload))
    conn.close()

app=FastAPI(title="Topologist Scheduler")

@app.post("/discover")
def discover(payload: dict = Body(...)):
    """
    payload:
      - {"device_id":"...", "depth":0, "auto_recursive":false, "max_depth":2}
      - หรือ {"host":"10.30.6.100", "username":"...", "password":"...", "depth":0}
    """
    if payload.get("device_id"):
        try: oid=ObjectId(payload["device_id"])
        except: raise HTTPException(400,"invalid device_id")
        dev = db.devices.find_one({"_id": oid})
        if not dev: raise HTTPException(404,"device not found")
        job={"type":"discovery","device_id":payload["device_id"],
             "depth": int(payload.get("depth", dev.get("depth",0))),
             "auto_recursive": bool(payload.get("auto_recursive", False)),
             "max_depth": int(payload.get("max_depth", 2))}
        enqueue("discovery", job)
        return {"queued":True,"device":dev["host"],"job":job}
    elif payload.get("host"):
        doc = {
            "host": payload["host"], "platform": payload.get("platform","cisco_ios"),
            "username": payload.get("username"), "password": payload.get("password"),
            "status": "ready" if payload.get("username") and payload.get("password") else "unknown",
            "depth": int(payload.get("depth",0)), "parent": payload.get("parent"),
            "created_at": time.time(), "last_seen": None
        }
        old = db.devices.find_one({"host": doc["host"]})
        if old:
            db.devices.update_one({"_id": old["_id"]},{"$set": doc})
            oid = old["_id"]
        else:
            oid = db.devices.insert_one(doc).inserted_id
        job={"type":"discovery","device_id":str(oid),
             "depth": doc["depth"], "auto_recursive": bool(payload.get("auto_recursive", False)),
             "max_depth": int(payload.get("max_depth", 2))}
        enqueue("discovery", job)
        return {"queued":True,"device_id":str(oid)}
    else:
        raise HTTPException(400,"device_id or host required")

@app.post("/discover_all")
def discover_all():
    """enqueue discovery สำหรับทุก device ที่พร้อม (status=ready)"""
    ready = list(db.devices.find({"status": "ready"}, {"_id": 1, "depth": 1}))
    count = 0
    for d in ready:
        job = {
            "type": "discovery",
            "device_id": str(d["_id"]),
            "depth": int(d.get("depth", 0)),
            "auto_recursive": False,
            "max_depth": 3
        }
        enqueue("discovery", job)
        count += 1
    return {"queued": count}