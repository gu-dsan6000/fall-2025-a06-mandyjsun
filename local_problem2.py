#!/usr/bin/env python3
"""
Problem 2: Cluster Usage Analysis (Local Version)

This script runs locally on the EC2 instance to test Spark logic.
It analyzes cluster usage patterns from Spark container logs to identify how many applications ran on each cluster, when they ran, and how long they lasted.

"""

import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql.functions import expr
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# Start Spark session
spark = (
    SparkSession.builder
    .appName("Problem2_ClusterUsage_Local")
    .master("local[*]")
    .getOrCreate()
)

# Initialize input and output paths
input_path = "data/sample/application_1485248649253_0052/*"
output_dir = "data/output"
os.makedirs(output_dir, exist_ok=True)

print(f"Reading logs from: {input_path}")

# Read text files into DataFrame
df = spark.read.text(input_path)

# Extract application_id and cluster_id from file paths
df = (
    df.withColumn("path", F.input_file_name())
      .withColumn("application_id", F.regexp_extract(F.col("path"), r"(application_\d+_\d+)", 1))
      .withColumn("cluster_id", F.regexp_extract(F.col("application_id"), r"application_(\d+)_", 1))
)

    # Extract timestamps from log lines and parse into proper timestamp column
df = df.withColumn(
        "timestamp_str",
        F.regexp_extract(F.col("value"), r"(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})", 1)
 )

    # Use try_to_timestamp to tolerate invalid input strings
df = df.withColumn(
        "timestamp",
        F.coalesce(
            expr("try_to_timestamp(timestamp_str, 'yy/MM/dd HH:mm:ss')"),
            expr("try_to_timestamp(timestamp_str, 'yyyy-MM-dd HH:mm:ss')"),
            expr("try_to_timestamp(timestamp_str, 'MM/dd/yyyy HH:mm:ss')")
        )
).filter(F.col("timestamp").isNotNull())

print(f"Total parsed log lines: {df.count():,}")

# Create per-application timeline (start and end times)
timeline_df = (
    df.groupBy("cluster_id", "application_id")
      .agg(
          F.min("timestamp").alias("start_time"),
          F.max("timestamp").alias("end_time")
      )
      .withColumn("app_number", F.lpad(F.split("application_id", "_").getItem(2), 4, "0"))
      .orderBy("cluster_id", "app_number")
)

timeline_pd = timeline_df.toPandas()
timeline_pd.to_csv(os.path.join(output_dir, "problem2_local_timeline.csv"), index=False)
print("Output to: data/output/problem2_local_timeline.csv")

# Create cluster summary (applications per cluster)
cluster_summary_df = (
    timeline_df.groupBy("cluster_id")
               .agg(
                   F.count("application_id").alias("num_applications"),
                   F.min("start_time").alias("cluster_first_app"),
                   F.max("end_time").alias("cluster_last_app")
               )
               .orderBy(F.desc("num_applications"))
)

cluster_summary_pd = cluster_summary_df.toPandas()
cluster_summary_pd.to_csv(os.path.join(output_dir, "problem2_local_cluster_summary.csv"), index=False)
print("Output to: data/output/problem2_local_cluster_summary.csv")

# Compute overall statistics
total_clusters = cluster_summary_df.count()
total_apps = cluster_summary_df.agg(F.sum("num_applications")).first()[0]
avg_apps = total_apps / total_clusters if total_clusters > 0 else 0

stats_lines = [
    f"Total unique clusters: {total_clusters}",
    f"Total applications: {total_apps}",
    f"Average applications per cluster: {avg_apps:.2f}",
    "",
    "Most heavily used clusters:"
]

top_clusters = cluster_summary_df.orderBy(F.desc("num_applications")).limit(5).collect()
for r in top_clusters:
    stats_lines.append(f"  Cluster {r['cluster_id']}: {r['num_applications']} applications")

stats_text = "\n".join(stats_lines)
print("\n" + stats_text)

with open(os.path.join(output_dir, "problem2_local_stats.txt"), "w") as f:
    f.write(stats_text)
print("Output to: data/output/problem2_local_stats.txt")

# Visualization 1: Bar chart – number of applications per cluster
plt.figure(figsize=(8, 5))
sns.barplot(data=cluster_summary_pd, x="cluster_id", y="num_applications", palette="viridis")
plt.title("Applications per Cluster (Local Sample)")
plt.xlabel("Cluster ID")
plt.ylabel("Number of Applications")
plt.xticks(rotation=45, ha="right")

for i, row in cluster_summary_pd.iterrows():
    plt.text(i, row.num_applications + 0.1, str(row.num_applications), ha="center", fontsize=8)

plt.tight_layout()
plt.savefig(os.path.join(output_dir, "problem2_local_bar_chart.png"))
plt.close()
print("Output to: data/output/problem2_local_bar_chart.png")

# Visualization 2: Density plot – job duration distribution
timeline_pd["duration_sec"] = (timeline_pd["end_time"] - timeline_pd["start_time"]).dt.total_seconds()
if not timeline_pd.empty:
    top_cluster = cluster_summary_pd.iloc[0]["cluster_id"]
    cluster_df = timeline_pd[timeline_pd["cluster_id"] == top_cluster]

    plt.figure(figsize=(8, 5))
    sns.histplot(cluster_df["duration_sec"], bins=30, kde=True, color="royalblue")
    plt.xscale("log")
    plt.xlabel("Job Duration (seconds, log scale)")
    plt.ylabel("Frequency")
    plt.title(f"Duration Distribution – Cluster {top_cluster} (n={len(cluster_df)})")
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "problem2_local_density_plot.png"))
    plt.close()
    print("Output to: data/output/problem2_local_density_plot.png")

# Stop Spark
spark.stop()
print("\nProblem 2 (Local Version) completed successfully.")
