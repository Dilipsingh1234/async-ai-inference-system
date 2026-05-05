# Asynchronous AI Inference System

## Project Overview

This project builds a simple asynchronous machine learning inference system using Airflow, AWS S3, AWS SQS, Docker, and Kubernetes.

The goal of the system is to train a machine learning model, send test records as inference jobs to a queue, process those jobs asynchronously using a consumer application, and store prediction results in S3.

The project focuses on MLOps system design and integration rather than model complexity.

---

## System Architecture

The system has two main flows:

### 1. Training Flow

Airflow is used to train a simple Scikit-learn model using the breast cancer dataset.

Workflow:

    Airflow Training DAG
            ↓
    Load breast cancer dataset
            ↓
    Split train/test data
            ↓
    Train Logistic Regression model
            ↓
    Save model as model.pkl
            ↓
    Upload model.pkl to S3

Output:

    s3://async-ai-inference-dilip/models/model.pkl

---

### 2. Inference Flow

Airflow sends test records to SQS. Each message contains one record for prediction.

Workflow:

    Airflow Queue DAG
            ↓
    Read test dataset
            ↓
    Send one message per record to SQS
            ↓
    Consumer reads message from SQS
            ↓
    Consumer loads model.pkl from S3
            ↓
    Consumer performs prediction
            ↓
    Consumer writes prediction JSON to S3
            ↓
    Consumer deletes message from SQS after success

Example SQS message:

    {
      "record_id": "sample_001",
      "features": [1.2, 3.4, 5.6]
    }

Example prediction output:

    {
      "record_id": "sample_001",
      "prediction": 1,
      "timestamp": "2026-04-15T12:00:00Z"
    }

Prediction files are stored separately in S3:

    s3://async-ai-inference-dilip/predictions/sample_001.json

---

## Project Structure

    async-ai-inference-system/
    │
    ├── dags/
    │   ├── train_model_dag.py
    │   └── populate_queue_dag.py
    │
    ├── consumer/
    │   ├── app.py
    │   └── requirements.txt
    │
    ├── k8s/
    │   └── consumer-deployment.yaml
    │
    ├── Dockerfile
    ├── README.md
    └── writeup.md

---

## Components

### Airflow

Airflow orchestrates the workflow.

It is used for:

- training the model
- uploading the trained model to S3
- creating inference jobs
- sending test records to SQS

### S3

S3 is used as cloud object storage.

It stores:

- the trained model file
- prediction result files

### SQS

SQS is used as the message queue.

It stores inference jobs until a consumer is ready to process them.

### Consumer Application

The consumer is a Python application that:

- polls SQS for messages
- downloads the trained model from S3
- performs inference
- writes prediction output to S3
- deletes the SQS message only after successful processing

### Docker

Docker packages the consumer application with all required dependencies.

### Kubernetes

Kubernetes is used to run the consumer as a scalable deployment.

The deployment starts with one replica and can be scaled to multiple replicas.

---

## Setup Instructions

### 1. Create and activate virtual environment

    python3 -m venv .venv
    source .venv/bin/activate

### 2. Install Python dependencies

Airflow was installed using constraints to avoid dependency conflicts.

    AIRFLOW_VERSION=2.10.2
    PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
    CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

    pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
    pip install scikit-learn==1.5.2 pandas==2.2.2 boto3==1.35.36 joblib

---

## Airflow Setup

Set Airflow home:

    export AIRFLOW_HOME=$(pwd)/airflow_home

Create DAG folder and copy DAG files:

    mkdir -p $AIRFLOW_HOME/dags
    cp dags/train_model_dag.py $AIRFLOW_HOME/dags/
    cp dags/populate_queue_dag.py $AIRFLOW_HOME/dags/

Initialize or migrate Airflow database:

    airflow db migrate

Check DAGs:

    airflow dags list

---

## Run Training DAG

Test the training task:

    airflow tasks test train_model_dag train_and_upload_model 2026-04-29

Expected output:

    Model uploaded to s3://async-ai-inference-dilip/models/model.pkl

Verify model in S3:

    aws s3 ls s3://async-ai-inference-dilip/models/

Expected file:

    model.pkl

---

## Run Queue Population DAG

Test the queue population task:

    airflow tasks test populate_queue_dag populate_sqs_queue 2026-04-30

This sends one message per test record to SQS.

Check approximate number of messages:

    aws sqs get-queue-attributes \
      --queue-url "https://sqs.us-east-1.amazonaws.com/471112754315/async-ai-inference-queue" \
      --attribute-names ApproximateNumberOfMessages

---

## Build Docker Image

Build the consumer Docker image:

    docker build -t async-ai-consumer:latest .

---

## Run Consumer with Docker

Run the Dockerized consumer:

    docker run --rm \
      -v ~/.aws:/root/.aws \
      -e AWS_DEFAULT_REGION="us-east-1" \
      -e AWS_REGION="us-east-1" \
      -e S3_BUCKET="async-ai-inference-dilip" \
      -e MODEL_S3_KEY="models/model.pkl" \
      -e SQS_QUEUE_URL="https://sqs.us-east-1.amazonaws.com/471112754315/async-ai-inference-queue" \
      -e PREDICTION_PREFIX="predictions/" \
      async-ai-consumer:latest

Expected behavior:

    Consumer downloads model.pkl from S3
    Consumer reads messages from SQS
    Consumer performs prediction
    Consumer writes prediction JSON files to S3
    Consumer deletes successfully processed messages from SQS

Check prediction outputs:

    aws s3 ls s3://async-ai-inference-dilip/predictions/

---

## Kubernetes Deployment

The Kubernetes deployment file is located at:

    k8s/consumer-deployment.yaml

Apply the deployment:

    kubectl apply -f k8s/consumer-deployment.yaml

Check pods:

    kubectl get pods

Check logs:

    kubectl logs deployment/async-ai-consumer

Scale consumers:

    kubectl scale deployment async-ai-consumer --replicas=3

Check deployment after scaling:

    kubectl get deployment async-ai-consumer

### Kubernetes Environment Note

`kubectl` was installed successfully in the Cloud9 environment. A local `kind` cluster was attempted, but the Cloud9 lab environment had limited disk/resources and could not fully initialize the Kubernetes control plane.

The Kubernetes YAML file is included and ready to deploy on a working Kubernetes cluster such as `kind`, `minikube`, or AWS EKS.

---

## Output Location

Model:

    s3://async-ai-inference-dilip/models/model.pkl

Predictions:

    s3://async-ai-inference-dilip/predictions/

---

## Summary

This project demonstrates an asynchronous ML inference system where Airflow creates training and inference workflows, S3 stores artifacts and results, SQS decouples job creation from processing, and Docker/Kubernetes package and run scalable consumers.

The system is designed so that inference jobs are processed asynchronously and reliably. Messages are deleted from SQS only after successful prediction and S3 write completion.