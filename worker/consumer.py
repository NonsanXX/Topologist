import os
import time
import json
import pika

RABBIT_HOST = os.getenv("RABBIT_HOST", "rabbitmq")


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


def enqueue_discovery(device_id, depth, auto_recursive, max_depth):
    """Enqueue a discovery job to RabbitMQ."""
    conn = connect_to_rabbitmq(max_retries=3, initial_delay=1)
    ch = conn.channel()
    ch.queue_declare(queue="discovery", durable=True)
    job = {
        "type": "discovery",
        "device_id": device_id,
        "depth": depth,
        "auto_recursive": auto_recursive,
        "max_depth": max_depth
    }
    ch.basic_publish(exchange="", routing_key="discovery", body=json.dumps(job))
    conn.close()


def trigger_discover_all(db):
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
