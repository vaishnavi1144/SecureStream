import json
import os
import random
import time
import uuid

from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer
from dotenv import load_dotenv


# -------------------------------------------------
# PATHS
# -------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent
CERT_DIR = BASE_DIR / "kafka" / "certs"


# -------------------------------------------------
# LOAD ENVIRONMENT VARIABLES
# -------------------------------------------------

load_dotenv(BASE_DIR / ".env")

KAFKA_BOOTSTRAP_SERVER = os.getenv(
    "KAFKA_BOOTSTRAP_SERVER",
    "localhost:9093"
)


# -------------------------------------------------
# SERVICES / SAMPLE DATA
# -------------------------------------------------

SERVICES = [
    "checkout-api",
    "payment-api",
    "authentication-service",
    "inventory-api"
]


SUCCESS_MESSAGES = [
    "Request completed successfully",
    "Database connection established",
    "User authentication completed",
    "Inventory lookup completed",
    "Payment request processed"
]


# -------------------------------------------------
# KAFKA PRODUCER CONFIGURATION
# -------------------------------------------------

producer_config = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVER,

    "security.protocol": "SSL",

    "ssl.ca.location":
        str(CERT_DIR / "ca.crt"),

    "ssl.certificate.location":
        str(CERT_DIR / "producer.crt"),

    "ssl.key.location":
        str(CERT_DIR / "producer.key")
}


producer = Producer(producer_config)


# -------------------------------------------------
# GENERATE APPLICATION LOG
# -------------------------------------------------

def generate_log():

    service = random.choice(SERVICES)

    status_code = random.choice([
        200,
        200,
        200,
        200,
        200,
        201,
        500,
        503
    ])

    response_time = random.randint(50, 5000)

    if status_code >= 500:
        level = "ERROR"
        message = "Service request failed"
    else:
        level = "INFO"
        message = random.choice(SUCCESS_MESSAGES)

    return {
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service": service,
        "environment": "production",
        "level": level,
        "status_code": status_code,
        "response_time_ms": response_time,
        "message": message
    }


# -------------------------------------------------
# KAFKA DELIVERY CALLBACK
# -------------------------------------------------

def delivery_report(err, msg):

    if err is not None:

        print(
            f"DELIVERY FAILED | "
            f"{err}"
        )

    else:

        print(
            f"DELIVERED | "
            f"topic={msg.topic()} | "
            f"partition={msg.partition()} | "
            f"offset={msg.offset()}"
        )


# -------------------------------------------------
# MAIN
# -------------------------------------------------

if __name__ == "__main__":

    print("SecureStream Kafka producer started...")
    print(f"Kafka broker: {KAFKA_BOOTSTRAP_SERVER}")

    try:

        while True:

            log = generate_log()

            print(
                f"{log['service']} | "
                f"{log['level']} | "
                f"{log['status_code']} | "
                f"{log['response_time_ms']} ms"
            )

            producer.produce(
                topic="application.logs",
                key=log["service"],
                value=json.dumps(log),
                callback=delivery_report
            )

            # Force delivery during testing so we immediately
            # know whether EC2 Kafka accepted the message.
            producer.flush(10)

            time.sleep(2)

    except KeyboardInterrupt:

        print("\nStopping producer...")

    finally:

        producer.flush()