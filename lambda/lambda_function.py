import json
import os
import boto3


s3 = boto3.client("s3")
sns = boto3.client("sns")


BUCKET_NAME = os.environ["BUCKET_NAME"]
SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]


def lambda_handler(event, context):

    print(f"Received event: {json.dumps(event)}")

    for record in event["Records"]:

        incident = json.loads(record["body"])

        event_id = incident["event_id"]

        service = incident.get("service", "unknown-service")

        print(
            f"Processing incident | "
            f"service={service} | "
            f"event_id={event_id}"
        )

        # Store the incident in S3
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=f"incidents/{service}/{event_id}.json",
            Body=json.dumps(incident, indent=2),
            ContentType="application/json"
        )

        print(
            f"Incident stored in S3 | "
            f"event_id={event_id}"
        )

        # Build notification
        alert_message = f"""
SecureStream Critical Incident

Service: {service}
Environment: {incident.get('environment')}
Level: {incident.get('level')}
HTTP Status: {incident.get('status_code')}
Response Time: {incident.get('response_time_ms')} ms
Message: {incident.get('message')}
Event ID: {event_id}
"""

        # Send alert
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=f"SecureStream Alert - {service}",
            Message=alert_message
        )

        print(
            f"SNS alert published | "
            f"event_id={event_id}"
        )

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "Incident processed successfully"
            }
        )
    }
