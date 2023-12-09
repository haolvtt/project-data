from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, ArrayType

if __name__ == "__main__":
    # Khởi tạo SparkSession
    spark = SparkSession.builder.appName("KafkaToElasticsearch").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Định nghĩa schema cho dữ liệu JSON
    json_schema = ArrayType(StructType([
        StructField("time", StringType(), True),
        StructField("open", IntegerType(), True),
        StructField("high", IntegerType(), True),
        StructField("low", IntegerType(), True),
        StructField("close", IntegerType(), True),
        StructField("volume", IntegerType(), True),
        StructField("ticker", StringType(), True)
    ]))

    # Định nghĩa tham số Kafka
    kafka_params = {
        "kafka.bootstrap.servers": "kafka:9092",
        "subscribe": "thanh-test",
        "startingOffsets": "latest"
    }

    # Đọc dữ liệu từ Kafka
    kafka_df = spark.readStream.format("kafka").options(**kafka_params).load()

    # Chuyển đổi cột 'value' từ dạng binary sang chuỗi JSON
    kafka_df = kafka_df.selectExpr("CAST(value AS STRING)")

    # Phân tích cú pháp chuỗi JSON và chuyển đổi thành DataFrame
    stock_df = kafka_df.select(from_json(col("value"), json_schema).alias("data"))

    # Sử dụng hàm explode để biến đổi mảng thành các hàng
    stock_df = stock_df.select(explode(col("data")).alias("stock_data")).select("stock_data.*")

    # Định nghĩa đường dẫn xuất HDFS
    output_path = "hdfs://namenode:8020/user/root/kafka_data"

    # Chỉ định vị trí checkpoint
    checkpoint_location_hdfs = "hdfs://namenode:8020/user/root/checkpoints_hdfs"
    checkpoint_location_es = "hdfs://namenode:8020/user/root/checkpoints_es"
    # Ghi dữ liệu vào HDFS dưới dạng file parquet
    hdfs_query = stock_df.writeStream \
        .outputMode("append") \
        .format("json") \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_location_hdfs) \
        .start()

    # Cấu hình ghi dữ liệu vào Elasticsearch
    es_write_conf = {
        "es.resource": "data_demo2",  # Thay thế 'index/type' bằng tên index và type của bạn trong Elasticsearch
        "es.nodes": "192.168.0.235",
        "es.port": "9200",
        "es.nodes.wan.only": "true"
    }

    # Ghi dữ liệu vào Elasticsearch
    es_query = stock_df.writeStream \
        .outputMode("append") \
        .format("org.elasticsearch.spark.sql") \
        .option("checkpointLocation", checkpoint_location_es) \
        .options(**es_write_conf) \
        .start()

    # Chờ đợi cả hai quá trình ghi hoàn tất
    hdfs_query.awaitTermination()
    es_query.awaitTermination()
