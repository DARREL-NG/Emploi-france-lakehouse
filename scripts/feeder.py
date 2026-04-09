# -*- coding: utf-8 -*-
"""
feeder.py - Couche Bronze / Raw
================================
Ingestion des donnees brutes vers HDFS /raw
- Source 1 : job_descriptions.csv (fichier CSV Kaggle)
- Source 2 : MySQL table chomage_dares (DARES)

Usage :
    spark-submit
        --master spark://spark-master:7077
        --deploy-mode client
        feeder.py
        --jobs_path /source/job_descriptions.csv
        --mysql_host mysql
        --mysql_db emploi_france
        --mysql_user spark
        --mysql_password spark123
        --hdfs_output hdfs://namenode:9000/raw
        --log_path /source/logs/feeder.log
"""

import argparse
import logging
import os
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit


# ─────────────────────────────────────────────
# 1. ARGUMENTS
# ─────────────────────────────────────────────
def parse_args():
    parser = argparse.ArgumentParser(description="Feeder — Ingestion vers HDFS /raw")
    parser.add_argument("--jobs_path",      required=True,  help="Chemin du fichier job_descriptions.csv")
    parser.add_argument("--mysql_host",     required=True,  help="Host MySQL (ex: mysql)")
    parser.add_argument("--mysql_db",       required=True,  help="Base de données MySQL (ex: emploi_france)")
    parser.add_argument("--mysql_user",     required=True,  help="Utilisateur MySQL")
    parser.add_argument("--mysql_password", required=True,  help="Mot de passe MySQL")
    parser.add_argument("--hdfs_output",    required=True,  help="Chemin HDFS de sortie (ex: hdfs://namenode:9000/raw)")
    parser.add_argument("--log_path",       required=True,  help="Chemin du fichier de log .txt")
    return parser.parse_args()


# ─────────────────────────────────────────────
# 2. LOGGER
# ─────────────────────────────────────────────
def setup_logger(log_path: str) -> logging.Logger:
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    logger = logging.getLogger("feeder")
    logger.setLevel(logging.INFO)

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Handler fichier .txt
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    # Handler console
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    return logger


# ─────────────────────────────────────────────
# 3. SPARK SESSION
# ─────────────────────────────────────────────
def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Feeder_Emploi_France")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.jars", "/source/mysql-connector-java-8.0.28.jar")
        .getOrCreate()
    )


# ─────────────────────────────────────────────
# 4. INGESTION OFFRES D'EMPLOI (Kaggle CSV)
# ─────────────────────────────────────────────
def ingest_jobs(spark: SparkSession, path: str, hdfs_output: str,
                year: str, month: str, day: str, logger: logging.Logger):
    logger.info("=" * 50)
    logger.info("[JOBS] Debut ingestion offres emploi Kaggle")
    logger.info(f"[JOBS] Source : {path}")

    try:
        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .option("multiLine", "true")
            .option("escape", '"')
            .option("quote", '"')
            .csv(path)
        )
        
        # Renommer les colonnes (supprimer espaces et caracteres speciaux)
        from pyspark.sql.functions import col
        new_columns = [c.strip()
                        .replace(' ', '_')
                        .replace('\r', '')
                        .replace('\n', '')
                        .replace(';', '')
                        .replace('{', '')
                        .replace('}', '')
                        .replace('(', '')
                        .replace(')', '')
                        .replace('=', '')
                       for c in df.columns]
        df = df.toDF(*new_columns)

        # Ajout colonnes de partition
        df = (df
              .withColumn("year",  lit(year))
              .withColumn("month", lit(month))
              .withColumn("day",   lit(day)))

        nb_rows = df.count()
        logger.info(f"[JOBS] Lignes lues      : {nb_rows}")
        logger.info(f"[JOBS] Colonnes         : {df.columns}")

        output_path = f"{hdfs_output}/offres_emploi"
        logger.info(f"[JOBS] Ecriture vers    : {output_path}")

        (df.write
           .mode("overwrite")
           .partitionBy("year", "month", "day")
           .parquet(output_path))

        logger.info(f"[JOBS] Ingestion OK — {nb_rows} lignes -> {output_path}")

    except Exception as e:
        logger.error(f"[JOBS] ERREUR : {str(e)}")
        raise


# ─────────────────────────────────────────────
# 5. INGESTION DARES (MySQL)
# ─────────────────────────────────────────────
def ingest_dares(spark: SparkSession, mysql_host: str, mysql_db: str,
                 mysql_user: str, mysql_password: str,
                 hdfs_output: str, year: str, month: str, day: str,
                 logger: logging.Logger):
    logger.info("=" * 50)
    logger.info("[DARES] Debut ingestion chomage depuis MySQL")
    logger.info(f"[DARES] Source : jdbc:mysql://{mysql_host}:3306/{mysql_db}")

    try:
        jdbc_url = f"jdbc:mysql://{mysql_host}:3306/{mysql_db}?useSSL=false&allowPublicKeyRetrieval=true&characterEncoding=UTF-8"

        df = (
            spark.read
            .format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", "chomage_dares")
            .option("user", mysql_user)
            .option("password", mysql_password)
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .load()
        )

        # Ajout colonnes de partition
        df = (df
              .withColumn("year",  lit(year))
              .withColumn("month", lit(month))
              .withColumn("day",   lit(day)))

        nb_rows = df.count()
        logger.info(f"[DARES] Lignes lues      : {nb_rows}")
        logger.info(f"[DARES] Colonnes         : {df.columns}")

        output_path = f"{hdfs_output}/chomage_dares"
        logger.info(f"[DARES] Ecriture vers    : {output_path}")

        (df.write
           .mode("overwrite")
           .partitionBy("year", "month", "day")
           .parquet(output_path))

        logger.info(f"[DARES] Ingestion OK — {nb_rows} lignes -> {output_path}")

    except Exception as e:
        logger.error(f"[DARES] ERREUR : {str(e)}")
        raise


# ─────────────────────────────────────────────
# 6. MAIN
# ─────────────────────────────────────────────
def main():
    args = parse_args()

    now   = datetime.now()
    year  = now.strftime("%Y")
    month = now.strftime("%m")
    day   = now.strftime("%d")

    logger = setup_logger(args.log_path)
    logger.info("=" * 50)
    logger.info("FEEDER — Debut de l'ingestion Bronze")
    logger.info(f"Date d'ingestion : {year}/{month}/{day}")
    logger.info(f"HDFS output      : {args.hdfs_output}")
    logger.info("=" * 50)

    spark = create_spark_session()
    logger.info(f"Spark version : {spark.version}")

    try:
        # Source 1 — Offres emploi Kaggle (CSV)
        ingest_jobs(
            spark=spark,
            path=args.jobs_path,
            hdfs_output=args.hdfs_output,
            year=year, month=month, day=day,
            logger=logger
        )

        # Source 2 — Chômage DARES (MySQL)
        ingest_dares(
            spark=spark,
            mysql_host=args.mysql_host,
            mysql_db=args.mysql_db,
            mysql_user=args.mysql_user,
            mysql_password=args.mysql_password,
            hdfs_output=args.hdfs_output,
            year=year, month=month, day=day,
            logger=logger
        )

        logger.info("=" * 50)
        logger.info("FEEDER — Ingestion Bronze completee avec succes !")
        logger.info(f"  {args.hdfs_output}/offres_emploi/year={year}/month={month}/day={day}/")
        logger.info(f"  {args.hdfs_output}/chomage_dares/year={year}/month={month}/day={day}/")
        logger.info("=" * 50)

    except Exception as e:
        logger.error(f"FEEDER — ECHEC GLOBAL : {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()
        logger.info("Spark session fermee.")


if __name__ == "__main__":
    main()