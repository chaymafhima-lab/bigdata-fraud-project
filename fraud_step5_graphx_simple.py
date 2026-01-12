# -*- coding: utf-8 -*-
# ==============================================================================
# FRAUD DETECTION - STEP 5 : ANALYSE DE GRAPHE (VERSION SIMPLIFIÉE)
# ==============================================================================
# Version compatible Python 2.7 sans GraphFrames
# Construction d'un graphe des relations entre transactions
# ==============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, round as spark_round

# ==============================================================================
# 1. INITIALISATION SPARK
# ==============================================================================
print("[INFO] Initialisation de la session Spark pour GraphX...")
spark = SparkSession.builder \
    .appName("FraudStep5_GraphAnalysis") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

print("[INFO] Session Spark creee avec succes.")

# ==============================================================================
# 2. CHARGEMENT DES DONNÉES
# ==============================================================================
print("\n[INFO] Chargement des donnees depuis HDFS...")
hdfs_path = "hdfs://namenode:8020/user/etudiant/fraud/clean/transactions_parquet"
df = spark.read.parquet(hdfs_path)

print("[INFO] Nombre total de transactions : {}".format(df.count()))
df.show(5)

# ==============================================================================
# 3. ANALYSE DES RELATIONS PAR MONTANT
# ==============================================================================
print("\n[INFO] Analyse des relations entre transactions...")

# Limiter pour la démo
df_sample = df.limit(50000)
print("[INFO] Utilisation d'un echantillon de {} transactions".format(df_sample.count()))

# Créer des buckets de montants pour regrouper les transactions similaires
print("\n[INFO] Creation des buckets de montants...")
df_with_bucket = df_sample.withColumn(
    "amount_bucket", 
    spark_round(col("Amount"), -1)
)

# ==============================================================================
# 4. IDENTIFICATION DES GROUPES (PSEUDO-GRAPHE)
# ==============================================================================
print("\n[INFO] Identification des groupes de transactions similaires...")

# Compter les transactions par bucket
groups = df_with_bucket.groupBy("amount_bucket", "Class") \
    .agg(count("*").alias("transaction_count")) \
    .orderBy(desc("transaction_count"))

print("\n[INFO] Top 20 groupes de transactions par montant :")
groups.show(20)

# ==============================================================================
# 5. DÉTECTION DES PATTERNS SUSPECTS
# ==============================================================================
print("\n[INFO] Detection des patterns suspects...")

# Groupes avec beaucoup de transactions (potentiellement suspects)
suspicious_groups = groups.filter(col("transaction_count") > 100)

print("[INFO] Nombre de groupes suspects : {}".format(suspicious_groups.count()))
print("\n[INFO] Groupes suspects (>100 transactions) :")
suspicious_groups.show(20)

# Distribution fraude/normal dans les groupes suspects
fraud_in_suspicious = suspicious_groups.groupBy("Class") \
    .agg(count("*").alias("num_groups"))

print("\n[INFO] Distribution fraude/normal dans les groupes suspects :")
fraud_in_suspicious.show()

# ==============================================================================
# 6. ANALYSE DES MONTANTS ÉLEVÉS
# ==============================================================================
print("\n[INFO] Analyse des montants eleves...")

high_amount_threshold = 200
high_amount_txn = df_sample.filter(col("Amount") > high_amount_threshold)

print("[INFO] Transactions avec montant > {} : {}".format(
    high_amount_threshold, 
    high_amount_txn.count()
))

high_amount_fraud = high_amount_txn.groupBy("Class") \
    .agg(count("*").alias("count"))

print("\n[INFO] Distribution fraude/normal pour montants eleves :")
high_amount_fraud.show()

# ==============================================================================
# 7. CENTRALITÉ : TRANSACTIONS LES PLUS CONNECTÉES
# ==============================================================================
print("\n[INFO] Identification des noeuds centraux (buckets les plus frequents)...")

# Les buckets avec le plus de transactions sont les "hubs"
central_nodes = groups.orderBy(desc("transaction_count")).limit(10)

print("\n[INFO] Top 10 noeuds centraux (buckets les plus connectes) :")
central_nodes.show(10, False)

# ==============================================================================
# 8. STATISTIQUES GLOBALES
# ==============================================================================
print("\n[INFO] Calcul des statistiques globales...")

total_groups = groups.count()
avg_group_size = groups.agg({"transaction_count": "avg"}).collect()[0][0]
max_group_size = groups.agg({"transaction_count": "max"}).collect()[0][0]

fraud_groups = groups.filter(col("Class") == 1).count()
normal_groups = groups.filter(col("Class") == 0).count()

print("\n" + "="*70)
print("STATISTIQUES DU GRAPHE (ANALYSE DE RELATIONS)")
print("="*70)
print("Transactions analysees    : {}".format(df_sample.count()))
print("Nombre de groupes         : {}".format(total_groups))
print("Taille moyenne groupe     : {:.2f}".format(avg_group_size))
print("Taille max groupe         : {}".format(int(max_group_size)))
print("Groupes suspects (>100)   : {}".format(suspicious_groups.count()))
print("Groupes avec fraude       : {}".format(fraud_groups))
print("Groupes normaux           : {}".format(normal_groups))
print("="*70)

# ==============================================================================
# 9. SAUVEGARDE DES RÉSULTATS
# ==============================================================================
print("\n[INFO] Sauvegarde des resultats dans HDFS...")

# Sauvegarder les groupes
groups_path = "hdfs://namenode:8020/user/etudiant/fraud/graphx/groups"
groups.write.mode("overwrite").parquet(groups_path)
print("[INFO] Groupes sauvegardes : {}".format(groups_path))

# Sauvegarder les nœuds suspects
suspicious_path = "hdfs://namenode:8020/user/etudiant/fraud/graphx/suspicious_groups"
suspicious_groups.write.mode("overwrite").parquet(suspicious_path)
print("[INFO] Groupes suspects sauvegardes : {}".format(suspicious_path))

# Sauvegarder les nœuds centraux
central_path = "hdfs://namenode:8020/user/etudiant/fraud/graphx/central_nodes"
central_nodes.write.mode("overwrite").parquet(central_path)
print("[INFO] Noeuds centraux sauvegardes : {}".format(central_path))

# ==============================================================================
# 10. ARRÊT
# ==============================================================================
print("\n[INFO] Arret de la session Spark...")
spark.stop()
print("[INFO] Termine !")
