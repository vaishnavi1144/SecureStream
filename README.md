# SecureStream
## Real-Time Application Log Monitoring & Incident Alerting Platform

SecureStream is a real-time application log monitoring and incident alerting platform built using **Python, Apache Kafka, Docker, and AWS**.

The platform generates application logs, securely publishes them to Apache Kafka using **mutual TLS (mTLS)**, processes the events in near real time using a containerized Python consumer running on **AWS ECS/Fargate**, stores application logs in **Amazon S3**, detects incidents, processes them asynchronously using **Amazon SQS and AWS Lambda**, and sends email notifications using **Amazon SNS**.

---

# 1. Problem Statement

Modern applications generate large volumes of logs across multiple services.

Manually reviewing these logs makes it difficult to quickly identify:

- HTTP server errors
- failed API requests
- authentication failures
- slow API responses
- critical application errors

SecureStream demonstrates how application events can be processed in near real time using an **event-driven cloud architecture**.

The platform automatically:

- generates application events
- streams events through Apache Kafka
- detects application incidents
- stores historical logs
- queues incidents for asynchronous processing
- stores incident records
- sends email alerts
- secures Kafka communication using certificates

---

# 2. Architecture

```mermaid
flowchart TD
    P["Python Log Producer<br/>Local Machine"]

    K["Apache Kafka<br/>Docker on Amazon EC2"]

    C["Python Kafka Consumer<br/>AWS ECS / Fargate"]

    SM["AWS Secrets Manager<br/>Kafka Client Certificates"]

    S3L["Amazon S3<br/>Application Logs"]

    SQS["Amazon SQS<br/>Incident Queue"]

    DLQ["Amazon SQS<br/>Dead-Letter Queue"]

    L["AWS Lambda<br/>Incident Processor"]

    S3I["Amazon S3<br/>Incident Records"]

    SNS["Amazon SNS<br/>Critical Alerts"]

    EMAIL["Email Notification"]

    P -->|"mTLS :9093"| K
    K -->|"mTLS :19093<br/>Private VPC"| C

    SM -->|"Retrieve certificates<br/>using ECS Task Role"| C

    C -->|"Store every log"| S3L
    C -->|"Detected incidents"| SQS

    SQS -->|"Failed after retries"| DLQ
    SQS -->|"Trigger"| L

    L -->|"Store incident"| S3I
    L -->|"Publish alert"| SNS

    SNS --> EMAIL
```

Kafka uses two SSL listeners:

```text
9093   -> Public Kafka listener used by the Python producer

19093  -> Private Kafka listener used by the ECS/Fargate consumer
```

The private Kafka listener is accessible only from the ECS consumer security group.

---

# 3. Technologies Used

## Application

- Python
- JSON
- Pytest

## Streaming

- Apache Kafka
- Kafka KRaft mode
- Confluent Kafka Python client

## Security

- SSL/TLS
- Mutual TLS authentication
- Custom Certificate Authority
- AWS Secrets Manager
- IAM roles and policies
- AWS Security Groups

## Containerization

- Docker
- Amazon Elastic Container Registry (ECR)

## AWS Services

- Amazon EC2
- Amazon ECS
- AWS Fargate
- Amazon ECR
- Amazon S3
- Amazon SQS
- Amazon SQS Dead-Letter Queue
- AWS Lambda
- Amazon SNS
- AWS Secrets Manager
- Amazon CloudWatch
- AWS IAM

---

# 4. Application Log Generation

The Python producer generates simulated application events for multiple services.

Example services:

```text
checkout-api
payment-api
authentication-service
inventory-api
```

Each generated event contains fields such as:

```json
{
  "event_id": "unique-event-id",
  "timestamp": "UTC timestamp",
  "service": "payment-api",
  "environment": "production",
  "level": "ERROR",
  "status_code": 503,
  "response_time_ms": 3900,
  "message": "Service request failed"
}
```

The producer continuously generates events with different:

- service names
- HTTP status codes
- response times
- log levels
- application messages

---

# 5. Kafka Event Streaming

The Python producer publishes application events to the Kafka topic:

```text
application.logs
```

Apache Kafka runs inside a Docker container hosted on an **Amazon EC2 instance**.

The producer connects to Kafka through:

```text
Port 9093
```

The Kafka EC2 security group restricts access to this port to the producer's allowed public IP address.

---

# 6. Mutual TLS Security

Kafka communication is secured using **mutual TLS authentication**.

Certificates were created for:

```text
Kafka Broker
Python Producer
Python Consumer
```

All certificates are signed by a custom Certificate Authority.

The producer uses:

```text
producer.crt
producer.key
ca.crt
```

The consumer uses:

```text
consumer.crt
consumer.key
ca.crt
```

The Kafka broker uses its own server certificate and private key.

With mutual TLS:

```text
Client verifies Kafka broker certificate
                +
Kafka broker verifies client certificate
```

This prevents unauthorized clients from publishing or consuming Kafka messages.

---

# 7. Negative Security Test

A separate producer test was executed without providing a valid client certificate.

Kafka rejected the connection with a TLS error similar to:

```text
certificate required
```

This verified that Kafka was correctly enforcing client-certificate authentication.

The test script is located at:

```text
producer/test_no_certificate.py
```

---

# 8. ECS/Fargate Consumer

The Python Kafka consumer is containerized using Docker.

The deployment flow is:

```text
consumer.py
     |
     v
Docker Image
     |
     v
Amazon ECR
     |
     v
ECS Task Definition
     |
     v
AWS Fargate
```

The consumer runs continuously inside ECS/Fargate without requiring a dedicated application server.

The ECS consumer connects to Kafka through the EC2 private DNS address using:

```text
Port 19093
```

This communication occurs inside the AWS VPC.

---

# 9. Secure Certificate Retrieval with AWS Secrets Manager

Kafka client certificates are **not stored inside the Docker image**.

Instead, they are stored securely in AWS Secrets Manager.

Secret name:

```text
securestream-kafka-client-certs
```

The secret contains Base64-encoded versions of:

```text
ca.crt
consumer.crt
consumer.key
```

When the ECS container starts:

```text
AWS Secrets Manager
        |
        v
ECS Task Role
        |
        v
Python Consumer
        |
        v
Decode Base64 data
        |
        v
/tmp/certs/ca.crt
/tmp/certs/consumer.crt
/tmp/certs/consumer.key
        |
        v
Kafka mTLS Connection
```

This prevents sensitive private keys from being embedded inside the Docker image.

---

# 10. Application Log Storage

Every application event consumed from Kafka is stored in Amazon S3.

The logs are organized by application service.

Example S3 structure:

```text
logs/
|
|-- checkout-api/
|
|-- payment-api/
|
|-- authentication-service/
|
|-- inventory-api/
```

Each event is stored using its unique event ID.

Example:

```text
logs/payment-api/<event-id>.json
```

This provides historical storage for previously processed application events.

---

# 11. Incident Detection

Each Kafka event is evaluated using predefined incident detection rules.

An incident is generated when one or more of the following conditions occur:

```text
HTTP status code >= 500

Response time > 3000 ms

Log level == CRITICAL
```

Examples of detected incidents include:

- HTTP 500 errors
- HTTP 503 errors
- slow API responses
- critical application errors

The detector can identify multiple incident reasons for the same event.

---

# 12. Amazon SQS Incident Queue

When the consumer detects an incident, it sends the event to:

```text
securestream-incidents
```

Amazon SQS decouples the Kafka consumer from downstream incident processing.

Instead of waiting for alert processing to finish, the consumer can continue processing additional Kafka events.

The flow becomes:

```text
Kafka
   |
   v
ECS Consumer
   |
   v
Incident Detected
   |
   v
Amazon SQS
```

---

# 13. Dead-Letter Queue

The incident queue is connected to a Dead-Letter Queue:

```text
securestream-incident-dlq
```

The configured maximum receive count is:

```text
3
```

If a message repeatedly fails processing, it can be moved to the Dead-Letter Queue.

This prevents problematic messages from repeatedly interrupting normal incident processing.

---

# 14. AWS Lambda Incident Processing

AWS Lambda is triggered by messages arriving in the SQS incident queue.

The Lambda function performs the following operations:

1. Receives the SQS message
2. Parses the incident JSON
3. Stores the incident in Amazon S3
4. Publishes an alert to Amazon SNS

Incident records are stored under:

```text
incidents/
```

Example structure:

```text
incidents/
|
|-- checkout-api/
|
|-- payment-api/
|
|-- authentication-service/
|
|-- inventory-api/
```

Example incident object:

```text
incidents/payment-api/<event-id>.json
```

---

# 15. Amazon SNS Email Alerts

AWS Lambda publishes incident notifications to the SNS topic:

```text
securestream-critical-alerts
```

A confirmed email subscription receives notifications when an application incident occurs.

The alert flow is:

```text
Incident
   |
   v
Amazon SQS
   |
   v
AWS Lambda
   |
   v
Amazon SNS
   |
   v
Email Alert
```

This notification mechanism could later be extended to systems such as:

- Slack
- Microsoft Teams
- PagerDuty
- ServiceNow
- SMS
- automated remediation systems

---

# 16. AWS Network Security

Kafka uses separate listeners for producer and consumer communication.

## Producer to Kafka

```text
Python Producer
      |
      | mTLS
      v
EC2 Kafka :9093
```

Port `9093` is restricted by the Kafka EC2 security group.

## ECS to Kafka

```text
ECS Consumer
      |
      | mTLS
      v
EC2 Kafka :19093
```

Port `19093` is restricted to:

```text
securestream-consumer-sg
```

The ECS consumer itself does not require any inbound application ports.

---

# 17. IAM Security

IAM roles are used so applications do not need hardcoded AWS credentials.

## ECS Task Role

The ECS application task role allows the consumer to perform operations such as:

```text
S3 PutObject

SQS SendMessage

SecretsManager GetSecretValue
```

## ECS Task Execution Role

The ECS execution role is responsible for infrastructure operations such as:

```text
Pulling Docker images from Amazon ECR

Sending container logs to CloudWatch
```

This separates application permissions from ECS infrastructure permissions.

---

# 18. CloudWatch Logging

The ECS/Fargate consumer sends its stdout and stderr logs to Amazon CloudWatch.

CloudWatch Log Group:

```text
/ecs/securestream-consumer
```

CloudWatch was used to verify events such as:

```text
Loading Kafka certificates from AWS Secrets Manager...

Kafka certificates loaded successfully.

Application events received

S3 objects stored

Incidents sent to SQS
```

CloudWatch also provides centralized troubleshooting information for the running ECS task.

---

# 19. Project Structure

```text
securestream/
|
|-- producer/
|   |
|   |-- producer.py
|   |
|   |-- test_no_certificate.py
|
|-- consumer/
|   |
|   |-- __init__.py
|   |
|   |-- consumer.py
|   |
|   |-- detector.py
|   |
|   |-- dockerfile
|
|-- Kafka/
|   |
|   |-- docker-compose.yml
|
|-- tests/
|   |
|   |-- test_detector.py
|
|-- requirements.txt
|
|-- .gitignore
|
|-- .dockerignore
|
|-- README.md
```

Certificate and private-key files are intentionally excluded from the repository.

---

# 20. Local Setup

Create a Python virtual environment:

```bash
python -m venv .venv
```

Activate the virtual environment on Windows PowerShell:

```powershell
.venv\Scripts\Activate.ps1
```

Install Python dependencies:

```bash
pip install -r requirements.txt
```

---

# 21. Environment Variables

The application uses environment variables for AWS and Kafka configuration.

Example:

```text
AWS_REGION=us-east-2

S3_BUCKET_NAME=<your-s3-bucket-name>

SQS_QUEUE_URL=<your-sqs-queue-url>

KAFKA_BOOTSTRAP_SERVER=<kafka-server>:9093
```

The actual `.env` file is intentionally excluded from Git.

---

# 22. Running the Producer

Start the Python producer using:

```bash
python producer/producer.py
```

Successful Kafka publishing produces output similar to:

```text
SecureStream Kafka producer started...

Kafka broker: <kafka-server>:9093

checkout-api | INFO | 200 | 3469 ms

DELIVERED | topic=application.logs | partition=0 | offset=0
```

The `DELIVERED` message confirms that Kafka successfully accepted the event.

---

# 23. Running the Consumer Locally

The consumer can also be run locally for testing:

```bash
python consumer/consumer.py
```

During local development, the consumer uses Kafka certificates stored in the local certificate directory.

When running in ECS:

```text
KAFKA_CERT_SOURCE=secretsmanager
```

causes the consumer to retrieve its certificates from AWS Secrets Manager instead.

---

# 24. Unit Testing

Incident detection logic is tested using Pytest.

Run:

```bash
python -m pytest -v
```

Tests include:

```text
Normal application request

HTTP server error

Slow API response

Multiple incident conditions
```

All implemented detector tests successfully passed.

---

# 25. Docker Build

Build the consumer Docker image using:

```bash
docker build -t securestream-consumer -f consumer/dockerfile .
```

The resulting image is pushed to Amazon ECR.

Example deployment flow:

```text
Source Code
    |
    v
Docker Build
    |
    v
Amazon ECR
    |
    v
ECS/Fargate
```

---

# 26. End-to-End Data Flow

The complete application flow is:

```text
Python Producer
       |
       | mTLS
       v
Apache Kafka on EC2
       |
       | mTLS
       v
ECS/Fargate Consumer
       |
       +----------------------+
       |                      |
       v                      v
Amazon S3                  Amazon SQS
Application Logs               |
                                v
                           AWS Lambda
                                |
                       +--------+--------+
                       |                 |
                       v                 v
                   Amazon S3         Amazon SNS
                   Incidents              |
                                          v
                                     Email Alert
```

---

# 27. Example Incident Flow

Example application event:

```text
Service: payment-api

Status Code: 503

Response Time: 3900 ms

Level: ERROR
```

Processing flow:

```text
payment-api event
       |
       v
Apache Kafka
       |
       v
ECS Consumer
       |
       +---------> S3 Application Log
       |
       v
Incident Detector
       |
       v
Amazon SQS
       |
       v
AWS Lambda
       |
       +---------> S3 Incident Record
       |
       v
Amazon SNS
       |
       v
Email Alert
```

---

# 28. End-to-End Validation

The complete SecureStream architecture was successfully tested.

The following were verified:

### Kafka Producer

```text
DELIVERED messages received
```

✅ Producer successfully published events to Kafka.

### Kafka Security

```text
mTLS connection established
```

✅ Kafka accepted authorized certificate-based clients.

### Invalid Client Test

```text
certificate required
```

✅ Kafka rejected a client that did not provide the required certificate.

### ECS/Fargate

```text
Task Status: RUNNING
```

✅ Containerized consumer successfully ran on AWS Fargate.

### AWS Secrets Manager

```text
Loading Kafka certificates from AWS Secrets Manager...

Kafka certificates loaded successfully.
```

✅ ECS retrieved Kafka client certificates securely at runtime.

### Kafka Consumer

✅ ECS consumed events from the Kafka broker through the private listener.

### Amazon S3

✅ New application log objects appeared under:

```text
logs/
```

### Incident Detection

✅ HTTP errors and slow API requests were successfully identified.

### Amazon SQS

✅ Detected incidents were sent to the SQS incident queue.

### AWS Lambda

✅ Lambda processed incident messages from SQS.

### Incident Storage

✅ New incident files appeared under:

```text
incidents/
```

### Amazon SNS

✅ Incident email notifications were successfully received.

---

# 29. Repository Security

Sensitive information is intentionally excluded from Git.

The `.gitignore` prevents files such as the following from being committed:

```text
.env

.venv/

Python cache files

Pytest cache

Private keys

Certificate signing requests

PKCS12 keystores

Kafka certificate directory

Secrets Manager JSON payload
```

Examples of files that should never be committed include:

```text
*.key

*.p12

ca.key

consumer.key

producer.key

securestream-kafka-secret.json
```

Base64 encoding is not encryption, so Base64-encoded private keys must also be treated as sensitive.

---

# 30. Future Improvements

Possible future enhancements include:

- OpenSearch integration for log search
- centralized dashboard for incidents
- Grafana dashboards
- Prometheus metrics
- Kafka Schema Registry
- retry and exponential backoff policies
- multiple Kafka brokers
- Kafka replication
- high-availability Kafka deployment
- Amazon MSK instead of self-managed Kafka
- Terraform infrastructure automation
- CI/CD pipeline for ECS deployments
- Slack alerts
- PagerDuty integration
- ServiceNow incident creation
- anomaly detection using machine learning
- automatic incident remediation

---

# 31. What This Project Demonstrates

SecureStream demonstrates practical experience with:

- Python backend development
- real-time event processing
- event-driven architecture
- Apache Kafka
- Kafka producers and consumers
- SSL/TLS certificates
- mutual TLS authentication
- custom Certificate Authorities
- Docker containerization
- Amazon EC2
- Amazon ECR
- Amazon ECS
- AWS Fargate
- Amazon S3
- Amazon SQS
- Dead-Letter Queues
- AWS Lambda
- Amazon SNS
- AWS Secrets Manager
- Amazon CloudWatch
- IAM permissions
- VPC networking
- AWS Security Groups
- asynchronous processing
- application monitoring
- incident detection
- unit testing
- secure cloud deployment

---

# 32. Summary

SecureStream implements a complete real-time application monitoring pipeline.

The system securely transports application events through Kafka, processes them using a containerized AWS Fargate consumer, stores historical application logs, detects failures and performance problems, processes incidents asynchronously, and sends automated alerts.

The final implementation successfully demonstrates:

```text
Secure Event Streaming
        +
Real-Time Processing
        +
Cloud Storage
        +
Incident Detection
        +
Asynchronous Processing
        +
Automated Alerting
        +
AWS Container Deployment
        +
Mutual TLS Security
```

The complete end-to-end pipeline was successfully validated from the Python producer through Kafka, ECS/Fargate, S3, SQS, Lambda, SNS, and final email notification.

# Validation Screenshots

## Kafka Producer Delivery

![Kafka producer delivery](screenshots/01-producer-delivered.png)

## ECS/Fargate Consumer

![ECS Fargate task](screenshots/02-ecs-task-running.png)

## CloudWatch Consumer Logs

![CloudWatch consumer logs](screenshots/03-cloudwatch-consumer-logs.png)

## S3 Application Logs

![S3 application logs](screenshots/04-s3-application-logs.png)

## S3 Incident Records

![S3 incident records](screenshots/05-s3-incidents.png)

## SNS Email Alert

![SNS email alert](screenshots/06-sns-email-alert.png)

