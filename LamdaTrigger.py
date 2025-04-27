import boto3
import json
import botocore
import time

# Capture start time at beginning of lambda_handler
start_time = time.time()

s3 = boto3.client('s3')
stepfunctions = boto3.client('stepfunctions')

# Replace with your actual Step Function ARN
STATE_MACHINE_ARN = '<arn StepFunction>'

def check_file_exists(bucket, key):
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False
        else:
            raise

def lambda_handler(event, context):
    # S3 bucket names
    orders_bucket = "<S3 bucket name>"
    returns_bucket = "<S3 bucket name>"

    # Extract file keys
    orders_s3_key = event.get("orders_s3_key")
    returns_s3_key = event.get("returns_s3_key")

    # Validate input keys
    if not orders_s3_key or not returns_s3_key:
        return {
            'statusCode': 400,
            'body': json.dumps({
                'message': '❌ Missing orders_s3_key or returns_s3_key in input.',
                'input': event
            })
        }

    # Check if files exist in S3
    orders_exists = check_file_exists(orders_bucket, orders_s3_key)
    returns_exists = check_file_exists(returns_bucket, returns_s3_key)

    if not orders_exists or not returns_exists:
        missing = []
        if not orders_exists:
            missing.append(f"{orders_bucket}/{orders_s3_key}")
        if not returns_exists:
            missing.append(f"{returns_bucket}/{returns_s3_key}")

        return {
            'statusCode': 404,
            'body': json.dumps({
                'message': '❌ One or both input files are missing in S3.',
                'missing_files': missing
            })
        }

    # ✅ Trigger Step Function instead of Glue
    try:
        step_response = stepfunctions.start_execution(
            stateMachineArn=STATE_MACHINE_ARN,
            input=json.dumps({
                "orders_s3_key": orders_s3_key,
                "returns_s3_key": returns_s3_key
            })
        )

        # After Step Function triggers successfully
        # end_time = time.time()
        # execution_time = round(end_time - start_time, 2)

        return {
        'statusCode': 200,
        'body': json.dumps({
            'message': '✅ Step Function triggered successfully',
            'executionArn': step_response['executionArn'],
            'orders_file': orders_s3_key,
            'returns_file': returns_s3_key,
            'execution_at': start_time
        })
}

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': '❌ Error triggering Step Function',
                'error': str(e)
            })
        }
