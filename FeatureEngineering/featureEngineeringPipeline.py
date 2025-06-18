from pyspark.sql import SparkSession
from timeSinceLastTransaction import add_time_since_last_transaction
from amountVelocitySpendingWindow import add_amount_velocity_features
import os

# Combine all features in a pipeline
def create_fraud_features(df):
    df = add_time_since_last_transaction(df)
    df = add_amount_velocity_features(df)
    return df


def main():
    spark = SparkSession.builder.appName("FraudDetection").getOrCreate()
    script_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(script_dir, "../data/creditcard.csv")
    df = spark.read.csv(csv_path, header=True, inferSchema=True)

    # Add all available features
    df = create_fraud_features(df)

    # Show a sample of the resulting DataFrame
    df.show(10)
    df.printSchema()

    # Optionally, save the result
    # df.write.parquet("../data/creditcard_features.parquet")

if __name__ == "__main__":
    main()