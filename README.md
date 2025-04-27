# Project Name: E-Commerce Data Processing Pipeline

## Overview
This project implements an automated data processing pipeline for an e-commerce platform, handling order and return data. The pipeline uses AWS services, including Lambda, Glue, Step Functions, and SNS, to process and analyze the data, making the results available via a Streamlit web application.

---

## Technologies : 
Python, AWS and Streamlit

---

## Features

- **Automated ETL pipeline**: Process order and return data using AWS Glue.
- **Orchestration with Step Functions**: Manage the flow of data processing with Step Functions.
- **AWS Lambda**: Trigger the ETL jobs when new data files are uploaded to S3.
- **Streamlit Web Interface**: Allow users to upload CSV files and track processing status.
- **Email Notifications**: Receive status updates on job success or failure via SNS.

---

## Architecture

The system architecture consists of the following components:
- **AWS S3**: Used to store the uploaded CSV files (orders and returns).
- **AWS Lambda**: Triggered after CSV uploads to initiate the processing pipeline.
- **AWS Step Functions**: Orchestrates the sequence of actions, including invoking AWS Glue jobs.
- **AWS Glue**: Performs ETL tasks, processing the data and joining orders and returns.
- **AWS SNS**: Sends notifications regarding the status of the processing.
- **Streamlit**: Provides a web interface for uploading files and tracking processing.

---

## Prerequisites

- **AWS Account**: Ensure that you have an AWS account with appropriate permissions to set up Lambda, Step Functions, Glue, SNS, and IAM roles.
- **AWS CLI**: Ensure that the AWS CLI is configured with the necessary access credentials.
- **Python 3.7+**: Required for running the Streamlit application and AWS SDKs.

---

## Setup Instructions

### 1. Set up AWS IAM Role
Create an IAM role with permissions for Lambda, Glue, Step Functions, and SNS to interact with the necessary AWS services.

### 2. Create AWS Lambda Function
Set up a Lambda function to trigger the AWS Step Function when 'orders' and 'returns' files are uploaded to S3. Ensure it has the required permissions to trigger Step Functions.

### 3. Create AWS Step Function
Define an AWS Step Function to orchestrate the workflow. The Lambda function triggers the Step Function, which then invokes the Glue job. Ensure the Step Function has appropriate permissions to invoke Glue jobs and manage the workflow.

### 4. Set up AWS Glue Job
Create a Glue job that processes the uploaded order and return data. If needed, establish a VPC connection to access your JDBC (e.g., Redshift). Make sure Glue has the required permissions to read from S3 and write to Redshift.

### 5. Set up AWS SNS Topic
Create an SNS topic to handle notifications (success and failure) and configure email subscriptions to receive status updates.

### 6. Run the Streamlit Application
Use the following command to run the Streamlit web application:
```bash
streamlit run [your_script.py]
