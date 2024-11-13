# GitHub Repository: Real-Time Offensive Language Filter on GCP with Spark and Bloom Filter

This repository demonstrates how to build and deploy a real-time offensive language filter on a Google Cloud Platform (GCP) cluster. The solution leverages PySpark, Hadoop, and Netcat to stream and filter text data, identifying and suppressing offensive words using a Bloom Filter. This setup is suitable for applications such as live chat moderation and social media monitoring, providing scalable real-time text filtering on GCP.

## Project Components

### Cluster Setup and File Storage on GCP

- **GCP Cluster**: A cluster was set up on GCP to run Spark jobs and access Hadoop Distributed File System (HDFS) for file storage.
- **Bloom Filter File Storage in HDFS**: Created a Bloom Filter file, stored it on HDFS within GCP, enabling distributed access for PySpark jobs.

### Bloom Filter Creation and Initialization (`create_bloom_filter.py`)

- **AFINN Word List Filtering**: Loaded and filtered an AFINN word list to identify offensive words (valence ≤ -4).
- **Bloom Filter Setup**: Created a Bloom Filter to flag offensive words, using hashing to populate the bit array and encoding it in Base64.
- **HDFS Storage**: Stored the Base64-encoded Bloom Filter on GCP’s HDFS, making it accessible for Spark jobs.

### Bad Word Filtering with PySpark (`filter_bad_words.py`)

- **Loading the Bloom Filter from HDFS**: Loaded and decoded the Bloom Filter from HDFS within a PySpark job.
- **Real-Time Filtering**: Used a Spark User Defined Function (UDF) to check each word in incoming sentences. If any offensive word matched the Bloom Filter, the sentence was suppressed; otherwise, it was passed along as clean.
- **Output**: The PySpark job processed the data and presented only clean sentences in the final output.

### Netcat for Live Data Streaming

- **Live Streaming**: Used Netcat to stream text data live (e.g., speeches) to simulate a real-time feed.
- **Integration with Spark**: Spark Streaming consumed this data, filtered it through the Bloom Filter, and displayed the filtered output, omitting sentences with offensive words.

## Workflow Summary

1. **Create and Store the Bloom Filter** on HDFS in GCP using `create_bloom_filter.py`, marking offensive words for real-time filtering.
2. **Set Up Netcat** on GCP to simulate a live stream of text data.
3. **Run PySpark Filtering Code** with `filter_bad_words.py`, which loads the Bloom Filter from HDFS, processes incoming sentences, and suppresses those containing offensive words.
4. **Output the Filtered Text** to the console, presenting a clean version of the live-streamed data.

This project showcases a scalable, real-time offensive language filter on GCP by combining the strengths of Hadoop's distributed storage, Spark’s real-time processing, and Netcat's streaming capabilities.
