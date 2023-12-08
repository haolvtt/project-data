from confluent_kafka import Consumer, KafkaException
import pandas as pd
import json
import time

def consume_kafka_to_dataframe(bootstrap_servers, topic_name):
    # Cấu hình Consumer
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': 'mygroup',
        'auto.offset.reset': 'earliest'
    }

    # Khởi tạo Consumer
    consumer = Consumer(conf)
    consumer.subscribe([topic_name])

    df_list = []  # List để lưu DataFrame

    last_message_time = time.time()  # Thời điểm nhận tin nhắn cuối cùng

    try:
        while True:
            # Nhận thông điệp từ Kafka
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # Kiểm tra xem đã hết thời gian 5 giây chưa
                if time.time() - last_message_time >= 5:
                    # Nếu đã hết 5 giây mà không có tin nhắn mới, ghép và in ra
                    if df_list:
                        result_df = pd.concat(df_list, ignore_index=True)
                        print("DataFrame gộp:")
                        print(result_df)
                        print("=" * 50)
                        df_list = []  # Reset list
                continue

            last_message_time = time.time()  # Cập nhật thời điểm nhận tin nhắn cuối cùng

            # Xử lý dữ liệu JSON từ Kafka message value
            json_data = msg.value().decode('utf-8')
            data_dict = json.loads(json_data)

            # Tạo DataFrame từ dữ liệu JSON và lưu vào list
            df = pd.DataFrame.from_dict(data_dict)
            df_list.append(df)

    except KeyboardInterrupt:
        pass

    finally:
        # Đóng Consumer
        consumer.close()

# Thông tin Kafka broker và tên topic bạn muốn lấy dữ liệu
bootstrap_servers = 'kafka:9092'  # Thay thế bằng địa chỉ Kafka broker của bạn
kafka_topic = 'thanh'  # Thay thế bằng tên Kafka topic của bạn

# Gọi hàm để lấy và in ra dữ liệu từ Kafka thành DataFrame
consume_kafka_to_dataframe(bootstrap_servers, kafka_topic)
