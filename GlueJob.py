import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import logging
from pyspark.sql.functions import current_timestamp

from pyspark.sql.functions import col

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get arguments from Step Function or Lambda
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'orders_s3_key',
    'returns_s3_key'
])

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Construct S3 paths
orders_path = f"s3://<S3 bucket name>/{args['orders_s3_key']}"
returns_path = f"s3://<S3 bucket name>/{args['returns_s3_key']}"

# Read data from S3
try:
    logger.info(f"📥 Reading Orders from: {orders_path}")
    logger.info(f"📥 Reading Returns from: {returns_path}")
    orders_df = spark.read.csv(orders_path, header=True, inferSchema=True)
    returns_df = spark.read.csv(returns_path, header=True, inferSchema=True)

    if orders_df.rdd.isEmpty() or returns_df.rdd.isEmpty():
        logger.error("❌ One or both input files are empty.")
        sys.exit(1)

except Exception as e:
    logger.error(f"❌ Error reading input files from S3: {str(e)}")
    sys.exit(1)

# Validate presence of required columns
if 'Order ID' not in orders_df.columns or 'Order ID' not in returns_df.columns:
    logger.error("❌ 'Order ID' column missing in one or both datasets.")
    sys.exit(1)

# Join datasets
joined_df = orders_df.join(returns_df, on="Order ID", how="left")
joined_df = joined_df.filter(col("Order ID").isNotNull())  # Drop rows with null Order ID
joined_df = joined_df.withColumn("UpdatedOn", current_timestamp())  # Add UpdatedOn column


# Write to S3
output_path = "s3://<S3 bucket name>/final/"
try:
    logger.info(f"📦 Writing joined data to: {output_path}")    
    joined_df.write.mode("overwrite").parquet(output_path)
except Exception as e:
    logger.error(f"❌ Error writing to S3: {str(e)}")
    sys.exit(1)

# Write to MySQL using JDBC
try:
    mysql_url = "jdbc:mysql://<***.amazonaws.com:3306>/<database>"
    joined_df.write \
        .format("jdbc") \
        .option("url", mysql_url) \
        .option("dbtable", "<Table name>") \
        .option("user", "<user name>") \
        .option("password", "<password>") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .mode("overwrite") \
        .save()
    logger.info("✅ Data successfully written to MySQL.")

except Exception as e:
    logger.error(f"❌ Error writing to MySQL: {str(e)}")
    sys.exit(1)

# Finalize job
job.commit()
