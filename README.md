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

## Commandes utiles (résumé)

- Lancer le cluster :
