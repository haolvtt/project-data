from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
def consume_kafka_message(spark, bootstrap_servers, topic_name):
    # Read from Kafka using PySpark Structured Streaming
    print("ALO 1")
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", topic_name) \
        .load()
    print("ALO 2")
    # Extract the value from the Kafka message
    df = df.selectExpr("CAST(value AS STRING)")
    print("ALO 3")
    # Print the content of the message to the console
    query = df.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()
    
    print("ALO 4")
    # Await termination
    query.awaitTermination()
    print("ALO 5")

if __name__ == "__main__":
    # Thông tin Kafka broker
    bootstrap_servers = 'kafka:9092'  # Thay thế bằng địa chỉ Kafka broker của bạn

    # Tên của topic bạn muốn nhận tin nhắn từ
    kafka_topic = 'my-topic'  # Thay thế bằng tên Kafka topic của bạn

    # Create a Spark session
    spark = SparkSession.builder \
        .appName("KafkaToConsole") \
        .getOrCreate()
    print("Before consume")
    # Gọi hàm để nhận tin nhắn từ topic
    consume_kafka_message(spark, bootstrap_servers, kafka_topic)
    print("After consume")
