# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg

spark = SparkSession.builder \
    .appName("FraudStep2_SQL") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# 1. Read the raw CSV from HDFS
df = spark.read.option("header", "True") \
               .option("inferSchema", "True") \
               .csv("hdfs://namenode:8020/user/etudiant/fraud/raw/creditcard.csv")

# 2. Basic statistics
total_tx = df.count()
fraud_tx = df.filter(col("Class") == 1).count()
non_fraud_tx = df.filter(col("Class") == 0).count()
avg_amount = df.agg(avg(col("Amount")).alias("avg_amount")).collect()[0]["avg_amount"]

print("Total transactions:", total_tx)
print("Fraud transactions:", fraud_tx)
print("Non fraud transactions:", non_fraud_tx)
print("Average amount:", avg_amount)

# 3. Create a clean dataset (here we just keep all columns as example)
clean_df = df

# 4. Write clean data in Parquet to HDFS
clean_df.write.mode("overwrite").parquet("hdfs://namenode:8020/user/etudiant/fraud/clean/transactions_parquet")

spark.stop()
