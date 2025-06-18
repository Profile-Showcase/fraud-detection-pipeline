from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Compute time since last transaction globally using 'Time' column
def add_time_since_last_transaction(df):
    window_spec = Window.orderBy("Time")
    df = df.withColumn(
        "prev_transaction_time",
        F.lag("Time", 1).over(window_spec)
    )
    df = df.withColumn(
        "time_since_last_transaction",
        F.when(
            F.col("prev_transaction_time").isNotNull(),
            F.col("Time") - F.col("prev_transaction_time")
        ).otherwise(None)
    )
    # Add derived features
    df = df.withColumn(
        "is_rapid_transaction",
        F.when(F.col("time_since_last_transaction") < 60, 1).otherwise(0)  # Less than 1 minute
    ).withColumn(
        "time_gap_zscore",
        (F.col("time_since_last_transaction") - F.avg("time_since_last_transaction").over(window_spec)) 
        / F.stddev("time_since_last_transaction").over(window_spec)
    )
    return df