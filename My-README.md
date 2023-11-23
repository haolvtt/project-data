## How to run

```
Step 1: git clone 'this repository'
Step 2: cd docker-hadoop-spark-workbench
Step 3: docker-compose up -d
Step 4: create file in hdfs
Step 5: write spark code in app/app.py
Step 6: run docker-compose build --no-cache spark-submit-app
Step 7: run docker-compose up -d
Step 8: Complete
```

-- tai file jar, nem vao container, chay code ben duoi app.py
-- code duoi dang bi loi ko doc dc

```
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

    df.printSchema()

    print("alo")

    readStringDF = df.selectExpr("CAST(value AS STRING)")

    print("ALO 3")
    # Print the content of the message to the console
    readStringDF.writeStream \
        .format("console") \
        .outputMode("append") \
        .start() \
        .awaitTermination()
    print("ALO 5")

if __name__ == "__main__":
    # Thông tin Kafka broker
    bootstrap_servers = 'kafka:9092'  # Thay thế bằng địa chỉ Kafka broker của bạn

    # Tên của topic bạn muốn nhận tin nhắn từ
    kafka_topic = 'my-topic'  # Thay thế bằng tên Kafka topic của bạn

    # Create a Spark session
    spark = SparkSession.builder \
        .appName("KafkaToConsole") \
        .config("spark.jars", "/spark-sql-kafka-0-10_2.12-3.3.0.jar") \
        .getOrCreate()
    print("Before consume")
    # Gọi hàm để nhận tin nhắn từ topic
    consume_kafka_message(spark, bootstrap_servers, kafka_topic)
    print("After consume")
```
