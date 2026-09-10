from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    avg,
    count,
    approx_count_distinct,
    sum,
    when,
    window,
    to_timestamp
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
)
import requests


METRICS_API_URL = "http://metrics-api:8000/metrics"


spark = (
    SparkSession.builder
    .appName("TelecomStreaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


schema = StructType([
    StructField("server_id", StringType()),
    StructField("cpu_usage", IntegerType()),
    StructField("latency_ms", IntegerType()),
    StructField("status", StringType()),
    StructField("timestamp", StringType())
])


df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "telecom_logs")
    .option("startingOffsets", "latest")
    .load()
)


json_df = df.selectExpr(
    "CAST(value AS STRING) AS value"
)


parsed_df = (
    json_df
    .select(
        from_json(col("value"), schema).alias("data")
    )
    .select("data.*")
    .withColumn(
        "timestamp",
        to_timestamp(col("timestamp"))
    )
)


metrics_df = (
    parsed_df
    .withWatermark("timestamp", "10 seconds")
    .groupBy(
        window(col("timestamp"), "10 seconds")
    )
    .agg(
        avg("cpu_usage").alias("avg_cpu"),
        avg("latency_ms").alias("avg_latency"),
        count("*").alias("total_events"),
        sum(
            when(col("status") == "ERROR", 1)
            .otherwise(0)
        ).alias("error_count"),
        approx_count_distinct("server_id").alias("unique_servers")
    )
    .withColumn(
        "error_rate",
        col("error_count") / col("total_events") * 100
    )
)


def process_batch(batch_df, batch_id):

    if batch_df.isEmpty():
        return

    latest = (
        batch_df
        .orderBy(col("window.end").desc())
        .limit(1)
        .collect()
    )

    if not latest:
        return

    row = latest[0]

    payload = {
        "window_start": row.window.start.strftime("%H:%M:%S"),
        "window_end": row.window.end.strftime("%H:%M:%S"),
        "avg_cpu": row.avg_cpu,
        "avg_latency": row.avg_latency,
        "total_events": row.total_events,
        "error_count": row.error_count,
        "error_rate": row.error_rate,
        "unique_servers": row.unique_servers
    }

    try:
        requests.post(METRICS_API_URL, json=payload, timeout=5)
    except requests.exceptions.RequestException as e:
        print(f"Failed to send metrics: {e}")


query = (
    metrics_df
    .writeStream
    .foreachBatch(process_batch)
    .outputMode("update")
    .start()
)

query.awaitTermination()