import sys
import base64
import hashlib
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

# Load Bloom Filter from a Base64-encoded file in HDFS using Spark's native methods
def load_bloom_filter(hdfs_path, spark):
    # Read the file as an RDD and collect it to the driver
    encoded_bloom = spark.sparkContext.textFile(hdfs_path).collect()[0].strip()  # Assuming it's a single-line file
    bit_array_bytes = base64.b64decode(encoded_bloom)
    
    # Convert the bytes directly into a list of bits
    bloom_filter = []
    for byte in bit_array_bytes:
        bits = format(byte, '08b')  # Convert each byte to its 8-bit binary representation
        bloom_filter.extend(int(bit) for bit in bits)
    
    return bloom_filter

# Hash function for Bloom Filter
def hash_word(word, seed, bloom_size):
    hash_obj = hashlib.md5((word + str(seed)).encode())
    return int(hash_obj.hexdigest(), 16) % 1500

# Check if a word might be in the Bloom Filter
def is_bad_word(word, bloom_filter, bloom_size, num_hashes=3):
    for seed in range(num_hashes):
        index = hash_word(word, seed, bloom_size)
        if bloom_filter[index] == 0:  # If any bit is not set, it's definitely not in the filter
            return False
    return True

# Spark UDF to filter sentences
def filter_sentence_udf(sentence):
    words = sentence.split(" ")
    for word in words:
        if is_bad_word(word, bloom_filter, bloom_size):
            return None  # Suppress the sentence if any bad word is found
    return sentence  # Pass the sentence if no bad words are found

# Main function for setting up the Spark Streaming application
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: filter_bad_words.py <hostname> <port>", file=sys.stderr)
        sys.exit(-1)

    host = sys.argv[1]
    port = int(sys.argv[2])

    # Spark session setup
    spark = SparkSession.builder.appName("BadWordFilterStream").getOrCreate()
    sc = spark.sparkContext
    
    # Load Bloom Filter from HDFS using Spark
    bloom_filter_path = "hdfs:///user/taralthota/bloom_filters/bloom_filter_base64.txt"  # Adjust to your HDFS path
    bloom_filter = load_bloom_filter(bloom_filter_path, spark)
    bloom_size = len(bloom_filter)

    # Check if bit at position 411 is set to 1 and print the result
    if bloom_filter[411] == 1:
        print("The bit at position 411 is set to 1.")
    else:
        print("The bit at position 411 is set to 0.")

    # Register the UDF
    filter_sentence = udf(filter_sentence_udf, StringType())

    # Set up streaming to listen for incoming sentences
    lines = spark.readStream.format("socket").option("host", host).option("port", port).load()

    # Apply the filtering UDF to each line
    filtered_lines = lines.withColumn("filtered_value", filter_sentence(col("value"))).where(col("filtered_value").isNotNull())

    # Display only clean sentences
    query = filtered_lines.select("filtered_value").writeStream.outputMode("append").format("console").option("truncate", "false").start()
    query.awaitTermination()
