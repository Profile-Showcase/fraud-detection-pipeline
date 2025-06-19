# Feature engineering functions for Spark DataFrames (Glue compatible)
from timeSinceLastTransaction import add_time_since_last_transaction
from amountVelocitySpendingWindow import add_amount_velocity_features

# Combine all features in a pipeline
def create_fraud_features(df):
    df = add_time_since_last_transaction(df)
    df = add_amount_velocity_features(df)
    return df

# Note: Do not include any SparkSession creation, file I/O, or main method here.
# This module is now ready to be imported into an AWS Glue ETL script.
# In Glue, use glueContext.spark_session to get the SparkSession and read/write from S3.
# Example usage in Glue:
# df = spark.read.csv('s3://your-bucket/input/', header=True, inferSchema=True)
# df = create_fraud_features(df)
# df.write.parquet('s3://your-bucket/output/')