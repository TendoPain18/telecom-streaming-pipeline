import random
import time
import json
from datetime import datetime, timezone
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="kafka:29092"
)

try:
    while True:
        server_id = f"server_{random.randint(1, 5):02d}"
        cpu_usage = random.randint(0, 100)
        latency_ms = random.randint(20, 500)
        status = random.choice(["OK", "ERROR"])

        timestamp = datetime.now(timezone.utc).isoformat()

        event = {
            "server_id": server_id,
            "cpu_usage": cpu_usage,
            "latency_ms": latency_ms,
            "status": status,
            "timestamp": timestamp
        }

        message = json.dumps(event)

        producer.send(
            "telecom_logs",
            value=message.encode("utf-8")
        )

        time.sleep(1)

except KeyboardInterrupt:
    pass

finally:
    producer.flush()
    producer.close()