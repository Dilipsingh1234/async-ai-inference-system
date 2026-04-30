from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import boto3

from sklearn.datasets import load_breast_cancer
from sklearn.model_selection import train_test_split


# -----------------------------
# Configuration
# -----------------------------

SQS_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/471112754315/async-ai-inference-queue"


def populate_sqs_queue():
    """
    This function reads test records and sends each one
    as a separate message to SQS.
    """

    # 1. Load dataset
    data = load_breast_cancer()
    X = data.data
    y = data.target

    # 2. Split the data same way as training DAG
    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.2,
        random_state=42
    )

    # 3. Create SQS client
    sqs = boto3.client("sqs")

    # 4. Send one message per test record
    for i, features in enumerate(X_test):
        message = {
            "record_id": f"sample_{i:03d}",
            "features": features.tolist()
        }

        sqs.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=json.dumps(message)
        )

        print(f"Sent message for {message['record_id']}")

    print(f"Total messages sent: {len(X_test)}")


with DAG(
    dag_id="populate_queue_dag",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    description="Send test dataset records to SQS for async inference",
) as dag:

    populate_queue_task = PythonOperator(
        task_id="populate_sqs_queue",
        python_callable=populate_sqs_queue
    )

    populate_queue_task