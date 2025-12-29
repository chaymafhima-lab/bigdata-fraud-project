# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("FraudStep1") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

df = spark.read.option("header", "True") \
               .option("inferSchema", "True") \
               .csv("hdfs://namenode:8020/user/etudiant/fraud/raw/creditcard.csv")

# Write a small sample to HDFS to check that everything works
df.limit(10).write.mode("overwrite").parquet("hdfs://namenode:8020/user/etudiant/fraud/test_output")

spark.stop()
