"""
Problem 1: Log Level Distribution (Cluster Version)

This script runs on the Spark cluster using the full dataset stored in S3. It connects to the Spark master,
reads all application container logs from the S3 bucket, extracts log levels (INFO, WARN, ERROR, DEBUG),
and writes summary outputs to the data/output/ directory on the master node.

"""

import os
import argparse
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, count as spark_count, input_file_name

# Parse arguments
parser = argparse.ArgumentParser(description="Problem 1: Log Level Distribution (Cluster Version)")
parser.add_argument("master", help="Spark master URL, e.g. spark://10.0.0.5:7077")
parser.add_argument("--net-id", required=True, help="Your NetID (used to locate your S3 bucket)")
args = parser.parse_args()

# Start Spark session
spark = (
    SparkSession.builder
    .appName("Problem1_LogLevelDistribution_Cluster")
    .master(args.master)
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
    .getOrCreate()
)

# Initialize S3 input path
input_path = f"s3a://{args.net_id}-assignment-spark-cluster-logs/data/application_*/*"
print(f"Reading log files from: {input_path}")

# Read all text files from S3 into a DataFrame
df = spark.read.text(input_path)
df = df.withColumn("file_path", input_file_name())

# Count total files and lines for validation
distinct_files = df.select("file_path").distinct().count()
print(f"Total files read: {distinct_files:,}")

total_lines = df.count()
print(f"Total lines read: {total_lines:,}")

# Show sample file paths and log lines
df.select("file_path").distinct().show(5, truncate=False)
df.show(3, truncate=False)

# Extract log levels (INFO, WARN, ERROR, DEBUG)
parsed = df.select(
    regexp_extract(
        "value",
        r"^\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} (INFO|WARN|ERROR|DEBUG)",
        1
    ).alias("log_level"),
    col("value").alias("log_entry")
).filter(col("log_level") != "")

parsed.show(5, truncate=False)

# Count logs by log level type and output into a CSV
counts_df = (
    parsed.groupBy("log_level")
    .agg(spark_count("*").alias("count"))
    .orderBy("log_level")
)
counts_pd = counts_df.toPandas()

os.makedirs("data/output", exist_ok=True)
counts_out = "data/output/problem1_counts.csv"
counts_pd.to_csv(counts_out, index=False)
print(f"Wrote: {counts_out}")

# Save a sample of parsed log entries
sample_pd = parsed.orderBy("log_level").limit(10).toPandas()
sample_out = "data/output/problem1_sample.csv"
sample_pd.to_csv(sample_out, index=False)
print(f"Wrote: {sample_out}")

# Generate and save summary stats
total_with_levels = parsed.count()
unique_levels = parsed.select("log_level").distinct().count()

summary_lines = [
    f"Total log lines processed: {total_lines}",
    f"Total lines with log levels: {total_with_levels}",
    f"Unique log levels found: {unique_levels}",
    "",
    "Log level distribution:"
]

# Compute percentages per log level
for row in counts_pd.itertuples(index=False):
    pct = (row.count / total_with_levels) * 100 if total_with_levels else 0
    summary_lines.append(f"  {row.log_level:<6}: {row.count:>10,} ({pct:6.2f}%)")

# Print summary for validation
summary_text = "\n".join(summary_lines)
print("\n" + summary_text)

# Write summary into a text file
summary_out = "data/output/problem1_summary.txt"
with open(summary_out, "w") as f:
    f.write(summary_text)
print(f"Wrote: {summary_out}")

# Stop Spark session
spark.stop()
print("Problem 1 (Cluster Version) completed successfully.")
