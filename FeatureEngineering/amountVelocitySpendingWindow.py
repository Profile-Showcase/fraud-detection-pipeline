from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Compute amount velocity (rolling sum/avg/count) globally using 'Time' and 'Amount' columns
def add_amount_velocity_features(df):
    # 1 hour window
    window_1hr = Window.orderBy("Time").rangeBetween(-3600, 0)
    df = df.withColumn(
        "amount_sum_1hr",
        F.sum("Amount").over(window_1hr)
    ).withColumn(
        "transaction_count_1hr",
        F.count("Amount").over(window_1hr)
    ).withColumn(
        "avg_amount_1hr",
        F.avg("Amount").over(window_1hr)
    )
    # 24 hour window
    window_24hr = Window.orderBy("Time").rangeBetween(-86400, 0)
    df = df.withColumn(
        "amount_sum_24hr",
        F.sum("Amount").over(window_24hr)
    ).withColumn(
        "transaction_count_24hr",
        F.count("Amount").over(window_24hr)
    ).withColumn(
        "avg_amount_24hr",
        F.avg("Amount").over(window_24hr)
    )
    # Ratio features
    df = df.withColumn(
        "amount_acceleration_1hr_to_24hr",
        F.col("amount_sum_1hr") / F.col("amount_sum_24hr")
    ).withColumn(
        "current_to_avg_ratio_24hr",
        F.col("Amount") / F.col("avg_amount_24hr")
    )
    return df

# Example usage:
# df = add_amount_velocity_features(df)