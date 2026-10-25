from confluent_kafka import Producer
import time

producer_config = {
    "bootstrap.servers": "localhost:9093",
    "security.protocol": "SSL",
    "ssl.ca.location": "kafka/certs/ca.crt"
}

producer = Producer(producer_config)


def delivery_report(err, msg):
    if err:
        print(f"EXPECTED FAILURE: {err}")
    else:
        print("UNEXPECTED: Message was delivered")


producer.produce(
    "application.logs",
    value="test-without-client-certificate",
    callback=delivery_report
)

producer.flush(10)

time.sleep(1)