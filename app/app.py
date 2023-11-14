import pyspark
from hdfs import InsecureClient
from pyspark.sql import SparkSession
print('spark library: ',pyspark.sql.__file__)
def count_words(content):
    word_count = {}
    words = content.split()

    for word in words:
        word = word.lower()
        word_count[word] = word_count.get(word, 0) + 1

    return word_count

def process_file(file_content):
    # Count words in the content
    word_count = count_words(file_content)

    # Convert word count to a Spark DataFrame for further processing
    spark = SparkSession.builder.appName("WordCount").getOrCreate()
    word_count_df = spark.createDataFrame(list(word_count.items()), ["word", "count"])

    # Perform any additional Spark operations as needed

    # Show the results
    word_count_df.show()

# Example usage
hdfs_file_path = '/user/root/alo.txt'

# Connect to HDFS and read content
hdfs_client = InsecureClient('http://namenode:50070', user='root')
with hdfs_client.read(hdfs_file_path) as file:
    file_content = file.read().decode('utf-8')
print('-----------Ready to process file-----------')
# Process the file using Spark
process_file(file_content)
