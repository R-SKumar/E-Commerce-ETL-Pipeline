import streamlit as st
import boto3
from datetime import datetime
import botocore
import json
import time
import io
import pandas as pd

# AWS Clients
s3 = boto3.client("s3")
lambda_client = boto3.client("lambda")
sfn_client = boto3.client("stepfunctions")

S3_BUCKETS = {
    "Orders": "<s3 order bucket name>",
    "Returns": "<s3 return bucket name>"
}

LAMBDA_NAME = "<Lamda Trigger Function Name>"
OUTPUT_BUCKET = "<s3 output bucket name>"
OUTPUT_PREFIX = "<s3 output folder name>"

# --- Initialize session state variables
if "uploaded_files" not in st.session_state:
    st.session_state.uploaded_files = {}
if "df_result" not in st.session_state:
    st.session_state.df_result = None
if "execution_arn" not in st.session_state:
    st.session_state.execution_arn = None
if "data_source" not in st.session_state:
    st.session_state.data_source = None
if "triggered" not in st.session_state:
    st.session_state.triggered = False
if "step_success" not in st.session_state:
    st.session_state.step_success = False
if "lambda_completed" not in st.session_state:
    st.session_state.lambda_completed = False
    
# --- Functions ---
def reset_session_state():
    defaults = {
        "uploaded_files": {},
        "df_result": None,
        "execution_arn": None,
        "data_source": None,
        "triggered": False,
        "step_success": False,
        "lambda_completed": False,
    }
    
    # Reset session state keys to defaults if they exist
    for key, default in defaults.items():
        if key in st.session_state:
            st.session_state[key] = default
        else:
            # Initialize the key if it doesn't exist
            st.session_state[key] = default
    
    # Notify user and refresh the app once
    st.success("🔄 Session state has been reset!")
    st.experimental_rerun()

def check_file_exists(bucket_name, key):
    try:
        s3.head_object(Bucket=bucket_name, Key=key)
        return True
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False
        else:
            raise

def get_execution_history(execution_arn):
    history = sfn_client.get_execution_history(
        executionArn=execution_arn,
        reverseOrder=False
    )
    events = history['events']
    steps = []
    for event in events:
        if 'stateEnteredEventDetails' in event:
            steps.append(f"➡️ Entered: {event['stateEnteredEventDetails']['name']}")
        elif 'stateExitedEventDetails' in event:
            steps.append(f"✅ Exited: {event['stateExitedEventDetails']['name']}")
        elif 'executionFailedEventDetails' in event:
            steps.append(f"❌ Failed: {event['executionFailedEventDetails']['error']}")
    return steps

def fetch_from_mysql():    
# JDBC is not exposed publicly, we will connect to it via a Lambda function to securely interact with the database.
    response = lambda_client.invoke(
        FunctionName="<lambda function name>",
        InvocationType='RequestResponse',
        Payload=json.dumps({})
    )
    result = json.load(response['Payload'])
    if result.get("statusCode") == 200:
        try:
            data_list = json.loads(result["body"])
            if isinstance(data_list, list):
                print("sql data available")
                return pd.DataFrame(data_list)
        except Exception as e:
            print(f"Error parsing body: {e}")
    print("no sql data")
    return None

def fetch_from_s3():
    try:
        # List objects in the bucket with the prefix
        response = s3.list_objects_v2(Bucket=OUTPUT_BUCKET, Prefix=OUTPUT_PREFIX)
        
        # Log the response for debugging
        # st.write("S3 List Objects Response:", response)

        # Check if 'Contents' key is in the response
        if 'Contents' not in response:
            st.error("❌ No objects found in S3 under the specified prefix.")
            return None
        
        # Filter out files that end with '.parquet'
        files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.parquet')]
        
        # Log the files for debugging
        st.write(f"Found {len(files)} Parquet file(s): {files}")

        if not files:
            st.error("❌ No Parquet files found in S3 output.")
            return None

        # Sort and get the latest file
        latest_file = sorted(files)[-1]
        st.write(f"Fetching the latest file: {latest_file}")

        # Fetch the file from S3
        obj = s3.get_object(Bucket=OUTPUT_BUCKET, Key=latest_file)
        
        # Read the parquet data
        df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
        st.write(f"Successfully fetched data with {df.shape[0]} rows and {df.shape[1]} columns.")
        return df

    except Exception as e:
        st.error(f"❌ Error fetching from S3: {str(e)}")
        return None

# --- RESET BUTTON ---
st.title("📦 Order & Return Data Uploader")

# Button Columns
col1, col2, col3 = st.columns([1, 3, 3]) 

# Reset Button - First column
with col1:
    if st.button("🔄Reset All"):
        reset_session_state()

status_placeholder = st.empty()

# Upload Orders File - Second column
with col2:
    orders_file = st.file_uploader("Upload Orders CSV", type=["csv"], key="orders")
    # Upload Orders file logic
    if orders_file and "orders_key" not in st.session_state.uploaded_files:
        orders_key = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{orders_file.name}"
        if check_file_exists(S3_BUCKETS["Orders"], orders_key):
            status_placeholder.warning(f"⚠️ This Orders file already exists in `{S3_BUCKETS['Orders']}`.")
        else:
            s3.upload_fileobj(orders_file, S3_BUCKETS["Orders"], orders_key)            
            st.session_state.uploaded_files["orders_key"] = orders_key
            status_placeholder.success(f"✅ Orders file uploaded to `{S3_BUCKETS['Orders']}` as `{orders_key}`")            

# Upload Returns File - Third column
with col3:
    returns_file = st.file_uploader("Upload Returns CSV", type=["csv"], key="returns")
    # Upload Returns file logic
    if returns_file and "returns_key" not in st.session_state.uploaded_files:
        returns_key = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{returns_file.name}"
        if check_file_exists(S3_BUCKETS["Returns"], returns_key):
            status_placeholder.warning(f"⚠️ This Returns file already exists in `{S3_BUCKETS['Returns']}`.")
        else:
            s3.upload_fileobj(returns_file, S3_BUCKETS["Returns"], returns_key)            
            st.session_state.uploaded_files["returns_key"] = returns_key
            status_placeholder.success(f"✅ Returns file uploaded to `{S3_BUCKETS['Returns']}` as `{returns_key}`")

# --- Start Processing ---
if "orders_key" in st.session_state.uploaded_files and "returns_key" in st.session_state.uploaded_files:
    status_placeholder.success(f"✅ Both files uploaded successfully!\n\n"
                        f"**Orders File :** {st.session_state.uploaded_files['orders_key']}\n\n"
                        f"**Returns File :** {st.session_state.uploaded_files['returns_key']}")

    if st.button("🚀 Start Data Processing"):
        payload = {
            "orders_s3_key": st.session_state.uploaded_files["orders_key"],
            "returns_s3_key": st.session_state.uploaded_files["returns_key"]
        }
        
        with st.spinner("Calling Lambda function..."):
            response = lambda_client.invoke(
                FunctionName=LAMBDA_NAME,   
                InvocationType="RequestResponse",
                Payload=json.dumps(payload)
            )
            
            # Read Lambda raw payload
            payload_stream = response["Payload"].read()
            lambda_result = json.loads(payload_stream)

            # Extract body
            lambda_body = {}
            if "body" in lambda_result:
                lambda_body = json.loads(lambda_result["body"])

            # Show success message
            st.success(f"✅ Lambda triggered successfully! Response:\n{lambda_body}")

            # Check for executionArn
            execution_arn = lambda_body.get("executionArn")
            if not execution_arn:
                st.error("❌ Lambda did not return a Step Function executionArn.")
            else:
                st.session_state.execution_arn = execution_arn
                # st.info(f"🔗 Execution ARN saved: {execution_arn}")
                
                # Set the triggered flag to True after Lambda execution
                st.session_state.triggered = True
    
                # --- POLL EXECUTION STATUS ---
                if st.session_state.get("triggered") and not st.session_state.get("lambda_completed"):
                    execution_arn = st.session_state.execution_arn

                    status_placeholder = st.empty()
                    steps_placeholder = st.empty()

                    with st.spinner("Polling Step Function status..."):
                        while True:
                            desc = sfn_client.describe_execution(executionArn=execution_arn)
                            status = desc["status"]
                            
                            # Update status
                            status_placeholder.markdown(f"**Execution Status:** `{status}`")
                            
                            # Update steps
                            steps = get_execution_history(execution_arn)
                            steps_placeholder.markdown("### 🧩 Step Function Progress\n" + "\n".join(steps))

                            if status in ["SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"]:
                                break
                            
                            time.sleep(5)

                    # Final result
                    if status == "SUCCEEDED":
                        st.success("✅ Step Function completed successfully!")
                        st.session_state.step_success = True
                    else:
                        st.error(f"❌ Step Function ended with status: {status}")

                    st.session_state.lambda_completed = True

# Final Output        
if st.session_state.step_success:
    st.markdown("### 📤 Choose data source to fetch final result:")
    data_source = st.radio("Select data source", ["S3", "MySQL"], horizontal=True)
    # data_source = "S3"

    if data_source != st.session_state.data_source:
        st.session_state.data_source = data_source
        st.session_state.df_result = None  # Reset result on change

        with st.spinner(f"Fetching from {data_source}..."):
            if data_source == "MySQL":
                df = fetch_from_mysql()
            else:
                df = fetch_from_s3()

            if df is not None:
                st.session_state.df_result = df
            else:
                st.warning("No data found.")

if st.session_state.df_result is not None:
    st.markdown("### 📊 Final Joined Orders & Returns Data")
    st.dataframe(st.session_state.df_result)
    
    csv_buffer = io.StringIO()
    st.session_state.df_result.to_csv(csv_buffer, index=False)
    st.download_button(
        label=f"⬇️ Download CSV from {st.session_state.data_source}",
        data=csv_buffer.getvalue(),
        file_name="joined_orders_returns.csv",
        mime="text/csv"
    )
            

