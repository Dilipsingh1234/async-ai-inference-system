import json
import os
import time
from datetime import datetime, timezone

import boto3
import joblib
import numpy as np


# -----------------------------
# Configuration
# -----------------------------

S3_BUCKET = os.getenv("S3_BUCKET", "async-ai-inference-dilip")
MODEL_S3_KEY = os.getenv("MODEL_S3_KEY", "models/model.pkl")
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/471112754315/async-ai-inference-queue")
PREDICTION_PREFIX = os.getenv("PREDICTION_PREFIX", "predictions/")


# -----------------------------
# AWS clients
# -----------------------------

s3 = boto3.client("s3")
sqs = boto3.client("sqs")


def download_model():
    """
    Download trained model from S3 and load it into memory.
    """

    local_model_path = "/tmp/model.pkl"

    print(f"Downloading model from s3://{S3_BUCKET}/{MODEL_S3_KEY}")

    s3.download_file(
        S3_BUCKET,
        MODEL_S3_KEY,
        local_model_path
    )

    model = joblib.load(local_model_path)

    print("Model loaded successfully")

    return model


def write_prediction_to_s3(record_id, prediction):
    """
    Write one prediction result as a separate JSON file to S3.
    """

    result = {
        "record_id": record_id,
        "prediction": int(prediction),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

    s3_key = f"{PREDICTION_PREFIX}{record_id}.json"

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=json.dumps(result),
        ContentType="application/json"
    )

    print(f"Prediction written to s3://{S3_BUCKET}/{s3_key}")


def process_message(model, message):
    """
    Process one SQS message:
    1. Read features
    2. Run prediction
    3. Save prediction to S3
    """

    body = json.loads(message["Body"])

    record_id = body["record_id"]
    features = body["features"]

    input_array = np.array(features).reshape(1, -1)

    prediction = model.predict(input_array)[0]

    write_prediction_to_s3(record_id, prediction)


def poll_sqs():
    """
    Continuously poll SQS for messages and process them.
    """

    model = download_model()

    print("Consumer started. Waiting for messages...")

    while True:
        response = sqs.receive_message(
            QueueUrl=SQS_QUEUE_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10
        )

        messages = response.get("Messages", [])

        if not messages:
            print("No messages found. Waiting...")
            time.sleep(2)
            continue

        for message in messages:
            try:
                process_message(model, message)

                sqs.delete_message(
                    QueueUrl=SQS_QUEUE_URL,
                    ReceiptHandle=message["ReceiptHandle"]
                )

                print("Message processed and deleted from SQS")

            except Exception as e:
                print(f"Error processing message: {e}")
                print("Message was NOT deleted, so it can be retried later")


if __name__ == "__main__":
    poll_sqs()