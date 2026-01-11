# -*- coding: utf-8 -*-
# ==============================================================================
# FRAUD DETECTION - STEP 4 : PRÉDICTION AVEC LE MODÈLE ENTRAÎNÉ
# ==============================================================================
# Ce script charge le modèle RandomForest entraîné précédemment et l'utilise
# pour effectuer des prédictions sur de nouvelles données de transactions.
#
# Étapes principales :
# 1. Charger le modèle depuis HDFS
# 2. Charger des données de test depuis HDFS
# 3. Préparer les features avec VectorAssembler
# 4. Effectuer des prédictions
# 5. Afficher les résultats et les statistiques
# ==============================================================================

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.sql.functions import col

# ==============================================================================
# 1. INITIALISATION DE LA SESSION SPARK
# ==============================================================================
print("[INFO] Initialisation de la session Spark...")
spark = SparkSession.builder \
    .appName("FraudStep4_Predict") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

print("[INFO] Session Spark creee avec succes.")

# ==============================================================================
# 2. CHARGEMENT DU MODÈLE DEPUIS HDFS
# ==============================================================================
# Le modèle a été sauvegardé lors de l'exécution de fraud_step3_ml.py
print("\n[INFO] Chargement du modele depuis HDFS...")
model_path = "hdfs://namenode:8020/user/etudiant/fraud/models/rf_fraud_model"

try:
    model = RandomForestClassificationModel.load(model_path)
    print("[INFO] Modele charge avec succes depuis : {}".format(model_path))
except Exception as e:
    print("[ERROR] Impossible de charger le modele : {}".format(e))
    print("[ERROR] Verifiez que fraud_step3_ml.py a ete execute avec succes.")
    spark.stop()
    exit(1)

# ==============================================================================
# 3. CHARGEMENT DES DONNÉES DE TEST
# ==============================================================================
# On peut utiliser le même dataset nettoyé ou un nouveau fichier CSV
print("\n[INFO] Chargement des donnees de test depuis HDFS...")
hdfs_path = "hdfs://namenode:8020/user/etudiant/fraud/clean/transactions_parquet"

df = spark.read.parquet(hdfs_path)

print("[INFO] Nombre total de lignes : {}".format(df.count()))
print("[INFO] Apercu des donnees :")
df.show(5)

# Si tu veux tester sur un échantillon seulement (pour aller plus vite) :
# df = df.limit(1000)

# ==============================================================================
# 4. PRÉPARATION DES FEATURES
# ==============================================================================
# On doit appliquer exactement la même transformation que lors de l'entraînement
print("\n[INFO] Assemblage des features...")

# Liste des colonnes de features (V1 à V28 + Amount)
feature_columns = ["V{}".format(i) for i in range(1, 29)] + ["Amount"]

# VectorAssembler : transforme plusieurs colonnes en un seul vecteur
assembler = VectorAssembler(
    inputCols=feature_columns,
    outputCol="features"  # Nom de la colonne attendue par le modèle
)

# Transformer le DataFrame pour ajouter la colonne "features"
df_assembled = assembler.transform(df)

print("[INFO] Features assemblees avec succes.")

# ==============================================================================
# 5. EFFECTUER DES PRÉDICTIONS
# ==============================================================================
print("\n[INFO] Generation des predictions...")

# Le modèle va ajouter les colonnes : prediction, rawPrediction, probability
predictions = model.transform(df_assembled)

print("[INFO] Predictions generees avec succes !")

# ==============================================================================
# 6. AFFICHAGE DES RÉSULTATS
# ==============================================================================
print("\n[INFO] Apercu des predictions (10 premieres lignes) :")
print("\nColonnes affichees :")
print("  - Class        : Label reel (0=normal, 1=fraude)")
print("  - prediction   : Label predit par le modele")
print("  - probability  : Probabilites [prob_class_0, prob_class_1]")
print("  - Amount       : Montant de la transaction\n")

predictions.select("Class", "prediction", "probability", "Amount").show(10, truncate=False)

# ==============================================================================
# 7. STATISTIQUES SUR LES PRÉDICTIONS
# ==============================================================================
print("\n[INFO] Statistiques des predictions...")

# Nombre total de transactions
total = predictions.count()

# Nombre de fraudes prédites
fraud_predicted = predictions.filter(col("prediction") == 1).count()

# Nombre de transactions normales prédites
normal_predicted = predictions.filter(col("prediction") == 0).count()

print("\n[STATS] Total de transactions analysees : {}".format(total))
print("[STATS] Transactions predites comme NORMALES (Class 0) : {} ({:.2f}%)".format(
    normal_predicted, 100*normal_predicted/float(total)))
print("[STATS] Transactions predites comme FRAUDES (Class 1)  : {} ({:.2f}%)".format(
    fraud_predicted, 100*fraud_predicted/float(total)))

# ==============================================================================
# 8. COMPARAISON AVEC LES VRAIS LABELS (si disponibles)
# ==============================================================================
# Si le dataset contient les vrais labels (colonne Class), on peut comparer
print("\n[INFO] Comparaison avec les vrais labels...")

# Vrais positifs (fraude réelle et prédite)
true_positives = predictions.filter((col("Class") == 1) & (col("prediction") == 1)).count()

# Vrais négatifs (normale réelle et prédite)
true_negatives = predictions.filter((col("Class") == 0) & (col("prediction") == 0)).count()

# Faux positifs (normale réelle mais prédite fraude)
false_positives = predictions.filter((col("Class") == 0) & (col("prediction") == 1)).count()

# Faux négatifs (fraude réelle mais prédite normale)
false_negatives = predictions.filter((col("Class") == 1) & (col("prediction") == 0)).count()

print("\n[CONFUSION] Vrais Positifs (fraude detectee correctement)  : {}".format(true_positives))
print("[CONFUSION] Vrais Negatifs (normale detectee correctement) : {}".format(true_negatives))
print("[CONFUSION] Faux Positifs (fausse alerte)                 : {}".format(false_positives))
print("[CONFUSION] Faux Negatifs (fraude non detectee)          : {}".format(false_negatives))

# ==============================================================================
# 9. SAUVEGARDE DES PRÉDICTIONS (OPTIONNEL)
# ==============================================================================
# Tu peux sauvegarder les prédictions dans HDFS pour analyse ultérieure
print("\n[INFO] Sauvegarde des predictions dans HDFS (optionnel)...")

output
c