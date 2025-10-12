import json
import pika


def enqueue(queue, payload):
    """Enqueue a message to RabbitMQ queue.
    
    Args:
        queue: Queue name
        payload: Dictionary payload to send
    """
    conn = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))
    ch = conn.channel()
    ch.queue_declare(queue=queue, durable=True)
    ch.basic_publish(exchange="", routing_key=queue, body=json.dumps(payload))
    conn.close()
