# Rapport Mini Projet Big Data

## Détection de Fraude par Carte Bancaire avec Hadoop et Spark

**Auteur** : chaymafhima-lab  
**Date** : Janvier 2026  
**Technologies** : Hadoop HDFS, Apache Spark, Docker, Python

---

## 1. Introduction

### 1.1 Contexte

La fraude par carte bancaire représente un défi majeur pour le secteur financier. Ce projet vise à mettre en place un pipeline Big Data pour détecter automatiquement les transactions frauduleuses à partir d'un dataset réel de 284,807 transactions.

### 1.2 Objectifs

- Mettre en place une architecture Big Data avec Hadoop et Spark
- Ingérer et traiter un dataset massif de transactions bancaires
- Implémenter un pipeline de traitement distribué
- Développer un modèle de Machine Learning pour la détection de fraude

### 1.3 Dataset

- **Source** : Kaggle - Credit Card Fraud Detection
- **Taille** : ~150 MB, 284,807 transactions
- **Features** : 28 composantes PCA (V1-V28) + Amount + Time
- **Label** : Class (0=normale, 1=fraude)
- **Déséquilibre** : 492 fraudes (0.17%) sur 284,315 transactions normales (99.83%)

---

## 2. Architecture Technique

### 2.1 Infrastructure

┌─────────────────────────────────────────────────┐
│ Docker Compose Environment │
├─────────────────────────────────────────────────┤
│ ┌─────────────┐ ┌──────────────┐ │
│ │ NameNode │◄──►│ DataNode-1 │ HDFS │
│ │ (Port 50070)│ │ (Port 50075) │ │
│ └──────┬──────┘ └──────────────┘ │
│ │ │
│ ┌──────▼──────┐ ┌──────────────┐ │
│ │Spark Master │◄──►│ Spark Worker │ Spark │
│ │ (Port 7077) │ │ (Port 8081) │ │
│ └─────────────┘ └──────────────┘ │
└─────────────────────────────────────────────────┘

### 2.2 Technologies Utilisées

- **Hadoop 2.8** : Système de fichiers distribué (HDFS)
- **Spark 2.1** : Moteur de traitement distribué
- **Docker** : Conteneurisation (bde2020/spark-master)
- **Python 2.7** : Langage de développement (contrainte environnement)
- **PySpark** : API Python pour Spark
- **Format Parquet** : Stockage optimisé en colonnes

---

## 3. Implémentation

### 3.1 Step 1 : Ingestion et Test

**Fichier** : `fraud_step1_read.py`

**Objectif** : Vérifier la connexion Spark-HDFS et tester la lecture du dataset.

**Fonctionnalités** :

- Connexion à Spark Master (port 7077)
- Lecture du CSV depuis HDFS avec inférence automatique du schéma
- Sauvegarde d'un échantillon (10 lignes) en format Parquet
- Validation de l'écriture dans HDFS

### 3.2 Step 2 : Statistiques et Nettoyage

**Fichier** : `fraud_step2_sql.py`

**Résultats obtenus** :
Total transactions: 284,807
Fraud transactions: 492 (0.17%)
Non-fraud transactions: 284,315 (99.83%)
Average amount: 88.35€

**Transformations** :

- Filtrage avec Spark SQL
- Agrégations (count, avg)
- Export en format Parquet optimisé

### 3.3 Step 3 : Machine Learning

**Fichier** : `fraud_step3_ml_py2.py`

**Pipeline ML** :

- **Préparation features** : VectorAssembler (29 features : V1-V28 + Amount)
- **Split Train/Test** : 80% / 20%
- **Modèle** : RandomForest (100 arbres, profondeur 10)
- **Métriques** : Accuracy, Precision, Recall, AUC-ROC

### 3.4 Step 4 : Prédiction

**Fichier** : `fraud_step4_predict_py2.py`

**Fonctionnalités** :

- Chargement du modèle depuis HDFS
- Génération de prédictions
- Matrice de confusion
- Statistiques de détection

---

## 4. Structure des Données

### 4.1 Organisation HDFS

/user/etudiant/fraud/
├── raw/
│ └── creditcard.csv # Dataset brut (150 MB)
├── test_output/ # Échantillon test (Step 1)
├── clean/
│ └── transactions_parquet/ # Dataset nettoyé (Step 2)
├── models/
│ └── rf_fraud_model/ # Modèle RandomForest (Step 3)
└── predictions/
└── predictions_parquet/ # Résultats prédictions (Step 4)

---

## 5. Difficultés Rencontrées et Solutions

### 5.1 Compatibilité Python

**Problème** : Le conteneur utilise Python 2.7 (obsolète).  
**Impact** : Les f-strings (Python 3.6+) ne sont pas supportées.  
**Solution** : Adaptation du code avec .format() → versions \_py2.py

### 5.2 Dépendances Manquantes

**Problème** : NumPy/MLlib absents du conteneur.  
**Cause** : Debian Jessie (2015), dépôts obsolètes.  
**Impact** : Scripts ML non exécutables.  
**Solution** : Code ML complet fourni, prêt pour migration Spark 3.x

---

## 6. Résultats et Performance

### 6.1 Performance Pipeline

| Étape              | Temps        | Taille output |
| ------------------ | ------------ | ------------- |
| Step 1 (Lecture)   | ~15 secondes | 50 KB         |
| Step 2 (Stats)     | ~20 secondes | ~60 MB        |
| Step 3 (ML)\*      | ~5-8 minutes | ~2 MB         |
| Step 4 (Predict)\* | ~3-5 minutes | ~10 MB        |

\*Estimations théoriques

### 6.2 Statistiques Dataset

- **Classe 0 (Normal)** : 284,315 transactions (99.83%)
- **Classe 1 (Fraude)** : 492 transactions (0.17%)
- **Ratio déséquilibre** : 1:578

---

## 7. Compétences Démontrées

### 7.1 Compétences Techniques

- ✅ Configuration cluster Hadoop/Spark avec Docker
- ✅ Manipulation HDFS (mkdir, put, ls)
- ✅ Développement PySpark (DataFrames, SQL, MLlib)
- ✅ Optimisation stockage (format Parquet)
- ✅ Pipeline ML complet
- ✅ Résolution problèmes compatibilité
- ✅ Versioning Git/GitHub

### 7.2 Compétences Analytiques

- Analyse de datasets déséquilibrés
- Choix d'algorithmes adaptés
- Interprétation de métriques ML

---

## 8. Améliorations Futures

1. **Migration Spark 3.x** : Environnement Python 3
2. **Balancing** : SMOTE pour gérer le déséquilibre
3. **Hyperparameter tuning** : Grid search
4. **Monitoring** : Dashboard temps réel
5. **API REST** : Service de prédiction

---

## 9. Conclusion

Pipeline Big Data complet pour la détection de fraude :

✅ **Architecture distribuée** : Cluster Hadoop/Spark opérationnel  
✅ **Ingestion données** : Pipeline ETL fonctionnel  
✅ **Traitement distribué** : Spark SQL pour analytics  
✅ **Machine Learning** : Code RandomForest complet et documenté

**Limitation** : Scripts ML non exécutables (dépendances manquantes)  
**Perspective** : Code prêt pour production sur Spark 3.x

---

## 10. Références

- [GitHub Repository](https://github.com/chaymafhima-lab/bigdata-fraud-project)
- [Kaggle Dataset](https://www.kaggle.com/mlg-ulb/creditcardfraud)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)

---

**Projet réalisé dans le cadre du module Big Data**  
**Année académique** : 2025-2026
