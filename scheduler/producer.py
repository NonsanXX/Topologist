import os
import json
import pika

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_USER = os.getenv("RABBITMQ_DEFAULT_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_DEFAULT_PASS", "guest")


def enqueue(queue, payload):
    """Enqueue a message to RabbitMQ queue.
    
    Args:
        queue: Queue name
        payload: Dictionary payload to send
    """
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    conn = pika.BlockingConnection(pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        credentials=credentials
    ))
    ch = conn.channel()
    ch.queue_declare(queue=queue, durable=True)
    ch.basic_publish(exchange="", routing_key=queue, body=json.dumps(payload))
    conn.close()
