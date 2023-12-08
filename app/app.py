from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

if __name__ == "__main__":
    # Turn off Spark logs
    spark = SparkSession.builder.appName("KafkaSparkDemo").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Define Kafka parameters
    kafka_params = {
        "kafka.bootstrap.servers": "kafka:9092",
        "subscribe": "thanh-test",
        "startingOffsets": "latest",
        "key.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
        "value.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
    }

    # Read data from Kafka as a streaming DataFrame
    kafka_stream_df = spark.readStream.format("kafka").options(**kafka_params).load()

    # Explicitly cast the 'value' column to STRING
    kafka_stream_df = kafka_stream_df.withColumn("value", expr("CAST(value AS STRING)"))

    # Define the HDFS output path
    output_path = "hdfs://namenode:8020/user/root/kafka_data"

    # Specify checkpoint location
    checkpoint_location = "hdfs://namenode:8020/user/root/checkpoints"

    # Write the streaming DataFrame to HDFS in JSON format, append to an existing file
    query = kafka_stream_df.writeStream \
        .outputMode("append") \
        .format("json") \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_location) \
        .start()

    query.awaitTermination()
