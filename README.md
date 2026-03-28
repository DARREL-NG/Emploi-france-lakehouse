# Emploi France Lakehouse

## Problématique business
**Quels métiers, secteurs et régions recrutent le plus en France ?**

Architecture Lakehouse Médaillon (Bronze → Silver → Gold) sur Hadoop/Spark.

## Sources de données
- **Kaggle** : `job_descriptions.csv` — 1 615 940 offres d'emploi mondiales
- **DARES** : `dares_defm_stock_france_cvs_trim.csv` — 17 832 lignes de données chômage France

## Architecture
```
Sources → feeder.py → HDFS /raw (Bronze)
                    → processor.py → HDFS /silver + Hive (Silver)
                                   → datamart.py → MySQL (Gold)
                                                 → API FastAPI
                                                 → Visualisations
```

## Stack technique
- **Hadoop 2.7.4** + **HDFS** — stockage distribué
- **Spark 3.0.0** — traitements distribués
- **Hive** — Data Warehouse Silver
- **MySQL** — Datamarts Gold
- **FastAPI + JWT** — API REST sécurisée
- **Matplotlib + Pandas** — Visualisations
- **Docker** — Environnement (image Marcel-Jan)

## Scripts

### feeder.py — Couche Bronze
Ingestion des données brutes vers HDFS /raw, partitionnées par year/month/day.
```bash
spark-submit --master spark://spark-master:7077 --deploy-mode client \
    --executor-memory 512m --driver-memory 1G --executor-cores 1 \
    --jars /source/mysql-connector-java-8.0.28.jar \
    /source/feeder.py \
    --jobs_path /source/job_descriptions.csv \
    --mysql_host mysql --mysql_db emploi_france \
    --mysql_user spark --mysql_password spark123 \
    --hdfs_output hdfs://namenode:9000/raw \
    --log_path /source/logs/feeder.log
```

### processor.py — Couche Silver
Validation (5 règles), jointure, agrégation, window function, cache().
```bash
spark-submit --master spark://spark-master:7077 --deploy-mode client \
    --executor-memory 512m --driver-memory 1G --executor-cores 1 \
    --jars /source/mysql-connector-java-8.0.28.jar \
    /source/processor.py \
    --raw_path hdfs://namenode:9000/raw \
    --silver_path hdfs://namenode:9000/silver \
    --log_path /source/logs/processor.log
```

### datamart.py — Couche Gold
3 datamarts MySQL : top métiers, chômage vs offres, profils demandés.
```bash
spark-submit --master spark://spark-master:7077 --deploy-mode client \
    --executor-memory 512m --driver-memory 1G --executor-cores 1 \
    --jars /source/mysql-connector-java-8.0.28.jar \
    /source/datamart.py \
    --silver_path hdfs://namenode:9000/silver \
    --mysql_host mysql --mysql_db emploi_france \
    --mysql_user spark --mysql_password spark123 \
    --log_path /source/logs/datamart.log
```

## API
```bash
cd api && uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```
- `POST /auth/token?username=admin&password=admin123` — obtenir JWT
- `GET /datamarts/top-metiers?page=1&page_size=20` — top métiers (paginé)
- `GET /datamarts/chomage-vs-offres` — chômage vs offres
- `GET /datamarts/profils-demandes` — profils demandés
- Swagger : `http://localhost:8000/docs`

## Visualisations
```bash
python3 viz/dashboard.py
```
3 graphiques générés dans `viz/output/` :
- `graph1_top_metiers.png` — Top 10 métiers par volume d'offres
- `graph2_contrats.png` — Répartition par type de contrat
- `graph3_pays.png` — Top 15 pays recruteurs

## Datamarts MySQL
| Table | Lignes | Description |
|-------|--------|-------------|
| datamart_top_metiers | 1 777 | Top 20 métiers par pays |
| datamart_chomage_vs_offres | 1 | Ratio offres/demandeurs |
| datamart_profils_demandes | 5 | Stats par type de contrat |

## Résultats clés
- **1 615 940** offres d'emploi ingérées
- **158 033** lignes après validation et agrégation Silver
- **UX/UI Designer** et **Software Engineer** sont les métiers les plus demandés
- **Full-Time** domine avec la plus forte moyenne d'offres par métier
