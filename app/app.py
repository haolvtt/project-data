from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split
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
        "subscribe": "your_topic_name",
        "startingOffsets": "earliest",  # You can adjust this based on your needs
    }

    # Read data from Kafka as a streaming DataFrame
    kafka_stream_df = spark.readStream.format("kafka").options(**kafka_params).load()

    # Extract value column and split into words
    words_df = kafka_stream_df.selectExpr("CAST(value AS STRING) as value").select(
        explode(split("value", " ")).alias("word")
    )

    # Perform word count
    word_count_df = words_df.groupBy("word").count()

    # Output the result to the console (for testing purposes)
    query = word_count_df.writeStream.outputMode("complete").format("console").start()

    # Wait for the streaming query to finish
    query.awaitTermination()
