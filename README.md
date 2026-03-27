# Emploi France Lakehouse

## Analyse du Marché de l'Emploi en France
> Quels métiers, secteurs et régions recrutent le plus en France ?

## Architecture Médaillon
- **Bronze** : HDFS /raw — données brutes (CSV Kaggle + MySQL DARES)
- **Silver** : HDFS /silver + Hive — données nettoyées
- **Gold**   : MySQL — datamarts métiers
- **Exposition** : API FastAPI + Visualisations

## Sources de données
- job_descriptions.csv — Offres d'emploi (Kaggle)
- DARES — Demandeurs d'emploi inscrits à France Travail (MySQL)

## Stack technique
- Hadoop 3.2.1 + Spark 3.0.0
- PySpark + Spark SQL + Hive
- MySQL 8.0
- FastAPI + JWT
- Matplotlib / Plotly

## Structure du projet
emploi-france-lakehouse/
├── scripts/
│   ├── feeder.py
│   ├── processor.py
│   └── datamart.py
├── api/
│   └── main.py
├── viz/
│   └── dashboard.py
├── logs/
├── data/
└── README.md
