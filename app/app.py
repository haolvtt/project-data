from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, window
from pyspark.sql.types import TimestampType
import logging

if __name__ == "__main__":
    # Tắt log của Spark
    spark = SparkSession.builder.appName("KafkaSparkDemo").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Bạn cũng có thể tắt log cho các thư viện khác như Kafka
    logger = logging.getLogger("py4j")
    logger.setLevel(logging.ERROR)

    # Define Kafka parameters
    kafka_params = {
        "kafka.bootstrap.servers": "kafka:9092",
        "subscribe": "thanh-test",
        "startingOffsets": "earliest",  # You can adjust this based on your needs
    }

    # Read data from Kafka as a streaming DataFrame
    kafka_stream_df = spark.readStream.format("kafka").options(**kafka_params).load()

    # Extract value column and split into words
    words_df = kafka_stream_df.selectExpr("CAST(value AS STRING) as value", "timestamp").select(
        explode(split("value", " ")).alias("word"),
        "timestamp"
    )

    # Apply watermark on eventTime column
    words_df = words_df.withColumn("eventTime", words_df.timestamp.cast(TimestampType()))

    # Perform word count with watermark
    word_count_df = words_df.withWatermark("eventTime", "10 minutes") \
        .groupBy(
            window("eventTime", "10 minutes", "5 minutes"),
            "word"
        ).count()

    # Output the result to the console (for testing purposes)
    # Define the HDFS output path
    output_path = "hdfs://namenode:8020/user/root/word_count"

    # Write the streaming DataFrame to HDFS in JSON format, append to existing file
    word_count_df.writeStream \
        .foreachBatch(lambda df, epoch_id: df.write.mode("append").json(output_path)) \
        .start() \
        .awaitTermination()