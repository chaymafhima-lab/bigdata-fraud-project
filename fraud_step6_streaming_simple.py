# -*- coding: utf-8 -*-
# ==============================================================================
# FRAUD DETECTION - STEP 6 : STREAMING - MONITORING EN TEMPS RÉEL
# ==============================================================================
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, when
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType

print("[INFO] Initialisation de la session Spark pour Streaming...")
spark = SparkSession.builder \
    .appName("FraudStep6_Streaming") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

print("[INFO] Session Spark creee avec succes.")

schema = StructType([
    StructField("Time", DoubleType(), True),
    StructField("V1", DoubleType(), True),
    StructField("V2", DoubleType(), True),
    StructField("V3", DoubleType(), True),
    StructField("V4", DoubleType(), True),
    StructField("V5", DoubleType(), True),
    StructField("V6", DoubleType(), True),
    StructField("V7", DoubleType(), True),
    StructField("V8", DoubleType(), True),
    StructField("V9", DoubleType(), True),
    StructField("V10", DoubleType(), True),
    StructField("V11", DoubleType(), True),
    StructField("V12", DoubleType(), True),
    StructField("V13", DoubleType(), True),
    StructField("V14", DoubleType(), True),
    StructField("V15", DoubleType(), True),
    StructField("V16", DoubleType(), True),
    StructField("V17", DoubleType(), True),
    StructField("V18", DoubleType(), True),
    StructField("V19", DoubleType(), True),
    StructField("V20", DoubleType(), True),
    StructField("V21", DoubleType(), True),
    StructField("V22", DoubleType(), True),
    StructField("V23", DoubleType(), True),
    StructField("V24", DoubleType(), True),
    StructField("V25", DoubleType(), True),
    StructField("V26", DoubleType(), True),
    StructField("V27", DoubleType(), True),
    StructField("V28", DoubleType(), True),
    StructField("Amount", DoubleType(), True),
    StructField("Class", IntegerType(), True)
])

print("\n[INFO] Configuration du streaming...")
input_path = "hdfs://namenode:8020/user/etudiant/fraud/streaming/input"
print("[INFO] Surveillance du dossier : {}".format(input_path))

streaming_df = spark.readStream \
    .option("header", "true") \
    .schema(schema) \
    .csv(input_path)

print("[INFO] Streaming configure avec succes.")

df_with_timestamp = streaming_df.withColumn("processing_time", current_timestamp())

df_analyzed = df_with_timestamp \
    .withColumn("is_fraud", col("Class")) \
    .withColumn("is_high_amount", when(col("Amount") > 100, 1).otherwise(0))

output_df = df_analyzed.select(
    col("Time"),
    col("Amount"),
    col("Class").alias("fraud_label"),
    col("is_high_amount"),
    col("processing_time")
)

print("\n[INFO] Configuration de la sortie streaming...")

output_path = "hdfs://namenode:8020/user/etudiant/fraud/streaming/output"
checkpoint_path = "hdfs://namenode:8020/user/etudiant/fraud/streaming/checkpoint"

query = output_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", output_path) \
    .option("checkpointLocation", checkpoint_path) \
    .trigger(processingTime="10 seconds") \
    .start()

print("\n" + "="*70)
print("STREAMING DEMARRE - MODE MONITORING")
print("="*70)
print("Surveillance      : {}".format(input_path))
print("Sortie            : {}".format(output_path))
print("Intervalle        : 10 secondes")
print("="*70)
print("\n[INFO] En attente de nouvelles transactions...")
print("[INFO] Appuyez sur Ctrl+C pour arreter.")

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\n[INFO] Arret du streaming...")
    query.stop()
    spark.stop()
    print("[INFO] Termine !")
