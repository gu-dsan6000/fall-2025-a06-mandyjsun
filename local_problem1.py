"""
Problem 1: Log Level Distribution (Local Version)

This script runs locally on the EC2 instance to test Spark logic. It reads log files
from data/sample/, extracts log levels (INFO, WARN, ERROR, DEBUG), and writes summary outputs to data/output/.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, count as spark_count, input_file_name
import pandas as pd

# Start spark session 
spark = (
    SparkSession.builder
    .appName("Problem1_LogLevelDistribution_Local")
    .master("local[*]")
    .getOrCreate()
)

# Initializing path 
input_path = "data/sample/"
print(f"Reading logs from: {input_path}")

# Read text from directory into a dataframe 
df = spark.read.text("data/sample/application_1485248649253_0052/*")
df = df.withColumn("file_path", input_file_name())

# Count files and lines for validation 
distinct_files = df.select("file_path").distinct().count()
print(f"Total files read: {distinct_files:,}")
total_lines = df.count()
print(f"Total lines read: {total_lines:,}")

# Quick preview
df.show(3, truncate=False)

# Parses the files, looks for log levels: Info, Warn, Error, and Debug 
parsed = df.select(
    regexp_extract(col("value"),
                   r"^\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} (INFO|WARN|ERROR|DEBUG)",
                   1).alias("log_level"),
    col("value").alias("log_entry")
).filter(col("log_level") != "")

parsed.show(5, truncate=False)

# Counts logs by log level type and outputs into a csv 
counts_df = (
    parsed.groupBy("log_level")
    .agg(spark_count("*").alias("count"))
    .orderBy("log_level")
)
counts_pd = counts_df.toPandas()

os.makedirs("data/output", exist_ok=True)
counts_out = "data/output/problem1_local_counts.csv"
counts_pd.to_csv(counts_out, index=False)

# Saves a sample of parsed log entries
sample_pd = parsed.orderBy("log_level").limit(10).toPandas()
sample_out = "data/output/problem1_local_sample.csv"
sample_pd.to_csv(sample_out, index=False)

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

# Prints summary into console for valiation 
summary_text = "\n".join(summary_lines)
print("\n" + summary_text)

# Writes summary into a txt file 
summary_out = "data/output/problem1_local_summary.txt"
with open(summary_out, "w") as f:
    f.write(summary_text)

# Stops spark session 
spark.stop()


