# bigdata-fraud-project

Mini projet Big Data : détection de fraude par carte bancaire avec Hadoop et Spark.

## Objectifs

- Ingestion du dataset **Credit Card Fraud Detection** (Kaggle) dans HDFS.
- Pré-traitement et statistiques avec **Spark SQL**.
- Production d’un dataset propre en **Parquet** pour l’entraînement d’un modèle MLlib.
- (Optionnel) Entraînement d’un modèle de classification fraude / non fraude avec Spark MLlib.

## Environnement technique

- Cluster Docker basé sur l’image **bde2020/spark-master** et **hadoop-namenode/datanode**.
- HDFS pour le stockage distribué.
- Spark (driver sur `spark-master`) pour le traitement.

## Scripts principaux

- `fraud_step1_read.py`

  - Lit `/user/etudiant/fraud/raw/creditcard.csv` depuis HDFS.
  - Écrit un échantillon en Parquet dans `/user/etudiant/fraud/test_output`.

- `fraud_step2_sql.py`
  - Lit le CSV brut depuis HDFS.
  - Calcule des statistiques de base (nombre total de transactions, nombre de fraudes, montant moyen, etc.).
  - Écrit un dataset nettoyé en Parquet dans `/user/etudiant/fraud/clean/transactions_parquet`.

- `fraud_step3_ml.py`

  - Charge le dataset nettoyé en Parquet depuis HDFS.
  - Prépare les features avec VectorAssembler (V1-V28 + Amount).
  - Divise les données en train/test (80/20).
  - Entraîne un modèle RandomForestClassifier avec 100 arbres.
  - Évalue le modèle avec Accuracy, Precision, Recall, AUC-ROC.
  - Sauvegarde le modèle entraîné dans `/user/etudiant/fraud/models/rf_fraud_model`.

- `fraud_step4_predict.py`

  - Charge le modèle entraîné depuis HDFS.
  - Lit les données de test (ou nouvelles transactions).
  - Prépare les features avec VectorAssembler.
  - Génère des prédictions (fraude / non-fraude).
  - Affiche les statistiques : nombre de fraudes détectées, matrice de confusion.
  - Sauvegarde les prédictions dans `/user/etudiant/fraud/predictions/predictions_parquet`.

## Commandes d'exécution

### 1. Lancer le cluster Docker
```bash
cd C:\Users\chaym\bigdata\docker-hadoop-spark-workbench
docker-compose up -d
```

### 2. Copier le dataset dans HDFS
```bash
# Copier le fichier CSV dans le conteneur namenode
docker cp "C:\Users\chaym\Downloads\creditcard.csv" namenode:/opt/data/creditcard.csv

# Se connecter au namenode et créer les répertoires HDFS
docker exec -it namenode bash
hdfs dfs -mkdir -p /user/etudiant/fraud/raw
hdfs dfs -put -f /opt/data/creditcard.csv /user/etudiant/fraud/raw/
exit
```

### 3. Exécuter les scripts Spark
```bash
# Se connecter au spark-master
docker exec -it spark-master bash

# Step 1 : Lecture et test
/spark/bin/spark-submit --master spark://spark-master:7077 /tmp/fraud_step1_read.py

# Step 2 : Statistiques et nettoyage
/spark/bin/spark-submit --master spark://spark-master:7077 /tmp/fraud_step2_sql.py

# Step 3 : Entraînement du modèle ML
/spark/bin/spark-submit --master spark://spark-master:7077 /tmp/fraud_step3_ml.py

# Step 4 : Prédictions
/spark/bin/spark-submit --master spark://spark-master:7077 /tmp/fraud_step4_predict.py
```

## Résultats attendus

- **Step 1** : Vérifie que HDFS est accessible et que Spark peut lire le CSV.
- **Step 2** : Produit des statistiques sur les fraudes et un dataset Parquet propre.
- **Step 3** : Entraîne un modèle RandomForest et affiche les métriques (Accuracy, AUC, etc.).
- **Step 4** : Génère des prédictions et affiche la matrice de confusion.

## Structure des répertoires HDFS

```
/user/etudiant/fraud/
├── raw/
│   └── creditcard.csv              # Dataset brut
├── test_output/                    # Output de test (Step 1)
├── clean/
│   └── transactions_parquet/       # Dataset nettoyé (Step 2)
├── models/
│   └── rf_fraud_model/             # Modèle entraîné (Step 3)
└── predictions/
    └── predictions_parquet/        # Prédictions (Step 4)
```

## Auteur

chaymafhima-lab

## Commandes utiles (résumé)

- Lancer le cluster :
