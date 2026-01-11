# -*- coding: utf-8 -*-
# ==============================================================================
# FRAUD DETECTION - STEP 3 : ENTRAÎNEMENT DU MODÈLE DE MACHINE LEARNING
# ==============================================================================
# Ce script entraîne un modèle de classification (RandomForest) pour détecter
# les transactions frauduleuses à partir du dataset nettoyé en Parquet.
#
# Étapes principales :
# 1. Charger les données depuis HDFS (format Parquet)
# 2. Préparer les features avec VectorAssembler
# 3. Diviser les données en train/test
# 4. Entraîner un RandomForestClassifier
# 5. Évaluer le modèle (Accuracy, Precision, Recall, AUC-ROC)
# 6. Sauvegarder le modèle entraîné dans HDFS
# ==============================================================================

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

# ==============================================================================
# 1. INITIALISATION DE LA SESSION SPARK
# ==============================================================================
print("[INFO] Initialisation de la session Spark...")
spark = SparkSession.builder \
    .appName("FraudStep3_ML") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

print("[INFO] Session Spark creee avec succes.")

# ==============================================================================
# 2. CHARGEMENT DES DONNÉES DEPUIS HDFS
# ==============================================================================
# Le fichier Parquet contient le dataset nettoyé produit par fraud_step2_sql.py
print("\n[INFO] Chargement des donnees depuis HDFS...")
hdfs_path = "hdfs://namenode:8020/user/etudiant/fraud/clean/transactions_parquet"

df = spark.read.parquet(hdfs_path)

print("[INFO] Nombre total de lignes : {}".format(df.count()))
print("[INFO] Apercu des premieres lignes :")
df.show(5)

# Affichage du schéma pour vérifier les colonnes disponibles
print("\n[INFO] Schema du DataFrame :")
df.printSchema()

# ==============================================================================
# 3. PRÉPARATION DES FEATURES POUR LE MODÈLE ML
# ==============================================================================
# Le dataset contient les colonnes V1, V2, ..., V28 (features PCA), Amount et Class
# On assemble toutes les features (V1-V28 + Amount) dans un vecteur unique
print("\n[INFO] Assemblage des features...")

# Liste des colonnes de features (V1 à V28 + Amount)
feature_columns = ["V{}".format(i) for i in range(1, 29)] + ["Amount"]

# VectorAssembler : transforme plusieurs colonnes en un seul vecteur de features
assembler = VectorAssembler(
    inputCols=feature_columns,
    outputCol="features"  # Nom de la colonne qui contiendra le vecteur
)

# Transformer le DataFrame pour ajouter la colonne "features"
df_assembled = assembler.transform(df)

print("[INFO] Features assemblees avec succes.")
print("[INFO] Apercu des donnees avec la colonne features :")
df_assembled.select("features", "Class").show(5, truncate=False)

# ==============================================================================
# 4. DIVISION DES DONNÉES EN ENSEMBLES TRAIN/TEST
# ==============================================================================
# On divise les données : 80% pour l'entraînement, 20% pour le test
print("\n[INFO] Division des donnees en train (80%) et test (20%)...")

train_data, test_data = df_assembled.randomSplit([0.8, 0.2], seed=42)

print("[INFO] Nombre de lignes dans train_data : {}".format(train_data.count()))
print("[INFO] Nombre de lignes dans test_data : {}".format(test_data.count()))

# ==============================================================================
# 5. ENTRAÎNEMENT DU MODÈLE RANDOMFORESTCLASSIFIER
# ==============================================================================
# RandomForest est un algorithme robuste pour la classification binaire
print("\n[INFO] Entrainement du modele RandomForestClassifier...")

rf = RandomForestClassifier(
    featuresCol="features",      # Colonne contenant les features
    labelCol="Class",             # Colonne contenant les labels (0=normal, 1=fraude)
    numTrees=100,                 # Nombre d'arbres dans la forêt
    maxDepth=10,                  # Profondeur maximale de chaque arbre
    seed=42                       # Graine pour la reproductibilité
)

# Entraîner le modèle sur les données d'entraînement
model = rf.fit(train_data)

print("[INFO] Modele entraine avec succes !")

# ==============================================================================
# 6. PRÉDICTIONS SUR L'ENSEMBLE DE TEST
# ==============================================================================
print("\n[INFO] Generation des predictions sur l'ensemble de test...")

predictions = model.transform(test_data)

# Afficher quelques prédictions
print("[INFO] Apercu des predictions :")
predictions.select("Class", "prediction", "probability").show(10, truncate=False)

# ==============================================================================
# 7. ÉVALUATION DU MODÈLE
# ==============================================================================
print("\n[INFO] Evaluation des performances du modele...")

# --- ACCURACY (Exactitude globale) ---
accuracy_evaluator = MulticlassClassificationEvaluator(
    labelCol="Class",
    predictionCol="prediction",
    metricName="accuracy"
)
accuracy = accuracy_evaluator.evaluate(predictions)
print("[METRIC] Accuracy : {:.4f}".format(accuracy))

# --- PRECISION (Précision) ---
precision_evaluator = MulticlassClassificationEvaluator(
    labelCol="Class",
    predictionCol="prediction",
    metricName="weightedPrecision"
)
precision = precision_evaluator.evaluate(predictions)
print("[METRIC] Precision : {:.4f}".format(precision))

# --- RECALL (Rappel) ---
recall_evaluator = MulticlassClassificationEvaluator(
    labelCol="Class",
    predictionCol="prediction",
    metricName="weightedRecall"
)
recall = recall_evaluator.evaluate(predictions)
print("[METRIC] Recall : {:.4f}".format(recall))

# --- AUC-ROC (Area Under ROC Curve) ---
auc_evaluator = BinaryClassificationEvaluator(
    labelCol="Class",
    rawPredictionCol="rawPrediction",
    metricName="areaUnderROC"
)
auc = auc_evaluator.evaluate(predictions)
print("[METRIC] AUC-ROC : {:.4f}".format(auc))

print("\n[INFO] Evaluation terminee.")

# ==============================================================================
# 8. SAUVEGARDE DU MODÈLE DANS HDFS
# ==============================================================================
print("\n[INFO] Sauvegarde du modele entraine dans HDFS...")

model_path = "hdfs://namenode:8020/user/etudiant/fraud/models/rf_fraud_model"
model.write().overwrite().save(model_path)

print("[INFO] Modele sauvegarde avec succes dans : {}".format(model_path))

# ==============================================================================
# 9. ARRÊT DE LA SESSION SPARK
# ==============================================================================
print("\n[INFO] Arret de la session Spark...")
spark.stop()
print("[INFO] Termine !")
