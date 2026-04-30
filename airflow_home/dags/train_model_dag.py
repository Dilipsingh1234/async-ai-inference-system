from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import joblib
import boto3

from sklearn.datasets import load_breast_cancer
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression


# -----------------------------
# Configuration
# -----------------------------

S3_BUCKET = "async-ai-inference-dilip"
MODEL_S3_KEY = "models/model.pkl"


def train_and_upload_model():
    """
    This function trains a simple machine learning model
    and uploads the saved model file to S3.
    """

    # 1. Load dataset
    data = load_breast_cancer()
    X = data.data
    y = data.target

    # 2. Split data into train and test
    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.2,
        random_state=42
    )

    # 3. Train model
    model = LogisticRegression(max_iter=5000)
    model.fit(X_train, y_train)

    # 4. Create local folder for model
    os.makedirs("/tmp/models", exist_ok=True)

    # 5. Save model locally
    local_model_path = "/tmp/models/model.pkl"
    joblib.dump(model, local_model_path)

    # 6. Upload model to S3
    s3 = boto3.client("s3")
    s3.upload_file(local_model_path, S3_BUCKET, MODEL_S3_KEY)

    print(f"Model uploaded to s3://{S3_BUCKET}/{MODEL_S3_KEY}")


# -----------------------------
# Airflow DAG definition
# -----------------------------

with DAG(
    dag_id="train_model_dag",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    description="Train sklearn model and upload model.pkl to S3",
) as dag:

    train_model_task = PythonOperator(
        task_id="train_and_upload_model",
        python_callable=train_and_upload_model
    )

    train_model_task