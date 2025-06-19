from awsglue.context import GlueContext
from pyspark.context import SparkContext
from featureEngineeringPipeline import create_fraud_features

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Read input data from S3
input_path = "s3://fraud-data-bucket-pip/input/"
df = spark.read.csv(input_path, header=True, inferSchema=True)

# Apply feature engineering
df = create_fraud_features(df)

# Write output to S3
output_path = "s3://fraud-data-bucket-pip/output/"
df.write.mode("overwrite").parquet(output_path)