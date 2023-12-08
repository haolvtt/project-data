from confluent_kafka import Producer
import json
from vnstock import *
from datetime import datetime, timedelta
import time

# Các dòng mã chứng khoán cần lấy dữ liệu
stock_array = ["ACB","BCM","BID","BVH","CTG","FPT","GAS","GVR","DHB","HPG","MBB","MSN",
               "MWG","PLX","POW","SAB","SHB","SSB","TCB","TPB","VCB","VHM","VIB","VIC","VJC","VNM","VPB","VRE"]

# Hàm gửi dữ liệu JSON vào Kafka với mã chứng khoán
def produce_kafka_json(bootstrap_servers, topic_name, symbol, json_message):
    producer = Producer({'bootstrap.servers': bootstrap_servers})
    producer.produce(topic_name, value=json_message.encode('utf-8'), key=symbol.encode('utf-8'), callback=delivery_report)
    producer.flush()

# Hàm callback cho việc gửi tin nhắn
def delivery_report(err, msg):
    """Callback được gọi khi tin nhắn được gửi thành công hoặc gặp lỗi."""
    if err is not None:
        print('Gửi tin nhắn thất bại: {}'.format(err))
    else:
        print('Tin nhắn được gửi thành công: {}'.format(msg.key().decode('utf-8')))

# Hàm lấy dữ liệu chứng khoán cho một mã cụ thể
def get_stock_data(symbol):
    today = datetime.now()
    start_date_this_week = today - timedelta(days=today.weekday())
    start_date_last_week = start_date_this_week - timedelta(days=7)
    end_date_last_week = start_date_last_week + timedelta(days=6)
    start_date_last_week_str = start_date_last_week.strftime('%Y-%m-%d')
    end_date_last_week_str = end_date_last_week.strftime('%Y-%m-%d')
    
    df = stock_historical_data(symbol=symbol,
                               start_date=start_date_last_week_str,
                               end_date=end_date_last_week_str,
                               resolution='1D',
                               type='stock',
                               beautify=True)
    # Chuyển dữ liệu thành JSON với thêm thông tin về mã chứng khoán
    df['time'] = pd.to_datetime(df['time'])
    df['time'] = df['time'].dt.strftime('%Y-%m-%d')
    json_data = df.to_json(date_format='iso', orient='records')
    return json_data

if __name__ == "__main__":
    bootstrap_servers = 'kafka:9092'  # Thay thế bằng địa chỉ Kafka broker của bạn
    kafka_topic = 'thanh-test'  # Thay thế bằng tên Kafka topic của bạn

    while True:
        for symbol in stock_array:
            stock_data = get_stock_data(symbol)  # Lấy dữ liệu cho mã chứng khoán hiện tại
            produce_kafka_json(bootstrap_servers, kafka_topic, symbol, stock_data)  # Gửi dữ liệu vào Kafka với mã chứng khoán
            time.sleep(2)  # Chờ 2 giây trước khi lấy dữ liệu cho mã chứng khoán tiếp theo
        time.sleep(60)

# from confluent_kafka import Producer
# import json
# from vnstock import *
# from datetime import datetime, timedelta
# import time

# def delivery_report(err, msg):
#     """Callback được gọi khi tin nhắn được gửi thành công hoặc gặp lỗi."""
#     if err is not None:
#         print('Gửi tin nhắn thất bại: {}'.format(err))
#     else:
#         print('Tin nhắn được gửi thành công: {}'.format(msg.value().decode('utf-8')))

# def produce_kafka_json(bootstrap_servers, topic_name, json_message):
#     # Khởi tạo producer
#     producer = Producer({'bootstrap.servers': bootstrap_servers})

#     # Gửi tin nhắn dưới dạng JSON vào Kafka topic mà không có key
#     producer.produce(topic_name, value=json_message.encode('utf-8'), callback=delivery_report)

#     # Chờ cho tất cả các tin nhắn được gửi và xác nhận
#     producer.flush()

# def get_stock_data():
#     # Lấy ngày hiện tại
#     today = datetime.now()

#     # Tính toán ngày 2 tháng trước
#     two_months_ago = today - timedelta(days=60)

#     # Chuyển định dạng ngày thành chuỗi 'YYYY-MM-DD'
#     start_date = two_months_ago.strftime('%Y-%m-%d')
#     end_date = today.strftime('%Y-%m-%d')

#     # Gọi hàm stock_historical_data với dữ liệu từ 2 tháng trước đến ngày hiện tại
#     df = stock_historical_data(symbol='GMD',
#                                start_date=start_date,
#                                end_date=end_date,
#                                resolution='1D',
#                                type='stock',
#                                beautify=True)

#     return df

# if __name__ == "__main__":
#     # Thông tin Kafka broker
#     bootstrap_servers = 'kafka:9092'  # Thay thế bằng địa chỉ Kafka broker của bạn


#     # Tên của topic bạn muốn gửi tin nhắn vào
#     kafka_topic = 'thanh-test'  # Thay thế bằng tên Kafka topic của bạn

#     # Lặp vô hạn để lấy dữ liệu mỗi 30 giây và gửi vào Kafka
#     while True:
#         stock_data = get_stock_data()  # Lấy dữ liệu từ hàm get_stock_data()
#         json_data = stock_data.to_json(date_format='iso', orient='records')

#         print(json_data)
#         # Gửi dữ liệu vào Kafka
#         produce_kafka_json(bootstrap_servers, kafka_topic, json_data)

#         # Chờ 30 giây trước khi lấy dữ liệu tiếp theo
#         time.sleep(30)
