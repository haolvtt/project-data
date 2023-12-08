from confluent_kafka import Producer
import json
from vnstock import *
from datetime import datetime, timedelta
import time

def delivery_report(err, msg):
    """Callback được gọi khi tin nhắn được gửi thành công hoặc gặp lỗi."""
    if err is not None:
        print('Gửi tin nhắn thất bại: {}'.format(err))
    else:
        print('Tin nhắn được gửi thành công: {}'.format(msg.value().decode('utf-8')))

def produce_kafka_json(bootstrap_servers, topic_name, json_message):
    # Khởi tạo producer
    producer = Producer({'bootstrap.servers': bootstrap_servers})

    # Gửi tin nhắn dưới dạng JSON vào Kafka topic mà không có key
    producer.produce(topic_name, value=json_message.encode('utf-8'), callback=delivery_report)

    # Chờ cho tất cả các tin nhắn được gửi và xác nhận
    producer.flush()

def get_stock_data():
    # Lấy ngày hiện tại
    today = datetime.now()

    # Tính toán ngày 2 tháng trước
    two_months_ago = today - timedelta(days=60)

    # Chuyển định dạng ngày thành chuỗi 'YYYY-MM-DD'
    start_date = two_months_ago.strftime('%Y-%m-%d')
    end_date = today.strftime('%Y-%m-%d')

    # Gọi hàm stock_historical_data với dữ liệu từ 2 tháng trước đến ngày hiện tại
    df = stock_historical_data(symbol='GMD',
                               start_date=start_date,
                               end_date=end_date,
                               resolution='1D',
                               type='stock',
                               beautify=True)

    return df

if __name__ == "__main__":
    # Thông tin Kafka broker
    bootstrap_servers = 'kafka:9092'  # Thay thế bằng địa chỉ Kafka broker của bạn


    # Tên của topic bạn muốn gửi tin nhắn vào
    kafka_topic = 'thanh'  # Thay thế bằng tên Kafka topic của bạn

    # Lặp vô hạn để lấy dữ liệu mỗi 30 giây và gửi vào Kafka
    while True:
        stock_data = get_stock_data()  # Lấy dữ liệu từ hàm get_stock_data()
        json_data = stock_data.to_json(date_format='iso', orient='records')

        print(json_data)
        # Gửi dữ liệu vào Kafka
        produce_kafka_json(bootstrap_servers, kafka_topic, json_data)

        # Chờ 30 giây trước khi lấy dữ liệu tiếp theo
        time.sleep(30)
