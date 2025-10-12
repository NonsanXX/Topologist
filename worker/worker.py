import json
import time
import pika

from database import db
from consumer import connect_to_rabbitmq
from callback import do_discovery_job


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
                if job.get("type") == "discovery":
                    do_discovery_job(job, db)
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


if __name__ == "__main__":
    consume()
