import base64
import json
import os
from pathlib import Path

import boto3
from dotenv import load_dotenv
from confluent_kafka import Consumer

from detector import detect_incident


# -------------------------------------------------
# PROJECT PATHS
# -------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent


# -------------------------------------------------
# LOAD ENVIRONMENT VARIABLES
# -------------------------------------------------

load_dotenv(BASE_DIR / ".env")

AWS_REGION = os.getenv("AWS_REGION")
BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
QUEUE_URL = os.getenv("SQS_QUEUE_URL")
KAFKA_BOOTSTRAP_SERVER = os.getenv(
    "KAFKA_BOOTSTRAP_SERVER",
    "localhost:9093"
)


# -------------------------------------------------
# CHECK REQUIRED ENVIRONMENT VARIABLES
# -------------------------------------------------

if not AWS_REGION:
    raise ValueError("AWS_REGION is missing from .env")

if not BUCKET_NAME:
    raise ValueError("S3_BUCKET_NAME is missing from .env")

if not QUEUE_URL:
    raise ValueError("SQS_QUEUE_URL is missing from .env")


# -------------------------------------------------
# AWS CLIENTS
# -------------------------------------------------

s3 = boto3.client(
    "s3",
    region_name=AWS_REGION
)

sqs = boto3.client(
    "sqs",
    region_name=AWS_REGION
)

KAFKA_CERT_SOURCE = os.getenv("KAFKA_CERT_SOURCE", "local")
KAFKA_SECRET_NAME = os.getenv(
    "KAFKA_SECRET_NAME",
    "securestream-kafka-client-certs"
)


# -------------------------------------------------
# KAFKA CERTIFICATES
# -------------------------------------------------


def prepare_kafka_certificates():
    # Local development: use existing cert files
    if KAFKA_CERT_SOURCE.lower() != "secretsmanager":
        return BASE_DIR / "kafka" / "certs"

    print("Loading Kafka certificates from AWS Secrets Manager...")

    secrets_client = boto3.client(
        "secretsmanager",
        region_name=AWS_REGION
    )

    response = secrets_client.get_secret_value(
        SecretId=KAFKA_SECRET_NAME
    )

    secret = json.loads(response["SecretString"])

    cert_dir = Path("/tmp/certs")
    cert_dir.mkdir(parents=True, exist_ok=True)

    files = {
        "ca_crt": "ca.crt",
        "consumer_crt": "consumer.crt",
        "consumer_key": "consumer.key"
    }

    for secret_key, filename in files.items():
        decoded = base64.b64decode(secret[secret_key])

        file_path = cert_dir / filename
        file_path.write_bytes(decoded)

    # Protect private key
    os.chmod(cert_dir / "consumer.key", 0o600)

    print("Kafka certificates loaded successfully.")

    return cert_dir

CERT_DIR = prepare_kafka_certificates()


# -------------------------------------------------
# KAFKA CONFIGURATION - mTLS
# -------------------------------------------------

consumer_config = {

    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVER,

    "group.id": "securestream-processors",

    "auto.offset.reset": "earliest",

    "security.protocol": "SSL",

    "ssl.ca.location":
        str(CERT_DIR / "ca.crt"),

    "ssl.certificate.location":
        str(CERT_DIR / "consumer.crt"),

    "ssl.key.location":
        str(CERT_DIR / "consumer.key")
}


# -------------------------------------------------
# STORE EVERY LOG IN S3
# -------------------------------------------------

def store_log_in_s3(log):

    event_id = log["event_id"]
    service = log["service"]

    key = f"logs/{service}/{event_id}.json"

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=json.dumps(log, indent=2).encode("utf-8"),
        ContentType="application/json"
    )

    print(
        f"S3 STORED | "
        f"{service} | "
        f"event_id={event_id}"
    )


# -------------------------------------------------
# SEND INCIDENT TO SQS
# -------------------------------------------------

def send_incident_to_sqs(log, reasons):

    incident = {
        **log,
        "incident_reasons": reasons
    }

    sqs.send_message(
        QueueUrl=QUEUE_URL,
        MessageBody=json.dumps(incident)
    )

    print(
        f"SQS SENT | "
        f"{log['service']} | "
        f"event_id={log['event_id']}"
    )


# -------------------------------------------------
# CREATE KAFKA CONSUMER
# -------------------------------------------------

consumer = Consumer(consumer_config)

consumer.subscribe([
    "application.logs"
])


print("SecureStream Kafka consumer started...")


# -------------------------------------------------
# MAIN PROCESSING LOOP
# -------------------------------------------------

try:

    while True:

        message = consumer.poll(1.0)

        if message is None:
            continue

        if message.error():

            print(
                f"Kafka consumer error: "
                f"{message.error()}"
            )

            continue


        # -----------------------------------------
        # Decode Kafka JSON message
        # -----------------------------------------

        log = json.loads(
            message.value().decode("utf-8")
        )


        # -----------------------------------------
        # Detect incident
        # -----------------------------------------

        result = detect_incident(log)


        # -----------------------------------------
        # Store every log in S3
        # -----------------------------------------

        store_log_in_s3(log)


        # -----------------------------------------
        # Handle incident
        # -----------------------------------------

        if result["incident"]:

            print(
                f"INCIDENT | "
                f"{log['service']} | "
                f"HTTP {log['status_code']} | "
                f"{log['response_time_ms']} ms | "
                f"Reasons: {', '.join(result['reasons'])} | "
                f"event_id={log['event_id']}"
            )


            # Send incident to SQS

            send_incident_to_sqs(
                log,
                result["reasons"]
            )


        else:

            print(
                f"NORMAL | "
                f"{log['service']} | "
                f"HTTP {log['status_code']} | "
                f"{log['response_time_ms']} ms | "
                f"event_id={log['event_id']}"
            )


except KeyboardInterrupt:

    print("\nStopping consumer...")


finally:

    consumer.close()