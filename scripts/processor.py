# -*- coding: utf-8 -*-
"""
processor.py - Couche Silver
=============================
Lecture depuis HDFS /raw
Nettoyage, validation, jointure, window function
Ecriture vers HDFS /silver + Hive

Usage :
    spark-submit
        --master spark://spark-master:7077
        --deploy-mode client
        processor.py
        --raw_path hdfs://namenode:9000/raw
        --silver_path hdfs://namenode:9000/silver
        --log_path /source/logs/processor.log
"""

import argparse
import logging
import os
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, trim, upper, when, count,
    avg, rank, to_date, regexp_extract
)
from pyspark.sql.window import Window


# -------------------------------------------------
# 1. ARGUMENTS
# -------------------------------------------------
def parse_args():
    parser = argparse.ArgumentParser(description="Processor - Couche Silver")
    parser.add_argument("--raw_path",    required=True, help="Chemin HDFS /raw")
    parser.add_argument("--silver_path", required=True, help="Chemin HDFS /silver")
    parser.add_argument("--log_path",    required=True, help="Chemin log .txt")
    return parser.parse_args()


# -------------------------------------------------
# 2. LOGGER
# -------------------------------------------------
def setup_logger(log_path):
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    logger = logging.getLogger("processor")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    return logger


# -------------------------------------------------
# 3. SPARK SESSION
# -------------------------------------------------
def create_spark_session():
    return (
        SparkSession.builder
        .appName("Processor_Emploi_France")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.jars", "/source/mysql-connector-java-8.0.28.jar")
        .enableHiveSupport()
        .getOrCreate()
    )


# -------------------------------------------------
# 4. LECTURE RAW
# -------------------------------------------------
def read_raw(spark, raw_path, logger):
    logger.info("[READ] Lecture offres emploi depuis /raw")
    df_jobs = spark.read.parquet(f"{raw_path}/offres_emploi")
    logger.info(f"[READ] Offres brutes : {df_jobs.count()} lignes")

    logger.info("[READ] Lecture chomage DARES depuis /raw")
    df_dares = spark.read.parquet(f"{raw_path}/chomage_dares")
    logger.info(f"[READ] DARES brutes : {df_dares.count()} lignes")

    return df_jobs, df_dares


# -------------------------------------------------
# 5. VALIDATION - 5 REGLES
# -------------------------------------------------
def validate_jobs(df, logger):
    logger.info("[VALID] Application des 5 regles de validation")
    total = df.count()

    # Regle 1 - Salary_Range non vide
    df = df.filter(col("Salary_Range").isNotNull() & (trim(col("Salary_Range")) != ""))
    logger.info(f"[VALID] Regle 1 - Salary_Range non vide : {df.count()} lignes")

    # Regle 2 - Job_Posting_Date valide format yyyy-MM-dd
    df = df.filter(col("Job_Posting_Date").isNotNull())
    df = df.withColumn("Job_Posting_Date", to_date(col("Job_Posting_Date"), "yyyy-MM-dd"))
    df = df.filter(col("Job_Posting_Date").isNotNull())
    logger.info(f"[VALID] Regle 2 - Date valide : {df.count()} lignes")

    # Regle 3 - Country non vide
    df = df.filter(col("Country").isNotNull() & (trim(col("Country")) != ""))
    logger.info(f"[VALID] Regle 3 - Country non vide : {df.count()} lignes")

    # Regle 4 - Work_Type valide
    valid_types = ["Full-Time", "Part-Time", "Contract", "Temporary", "Intern"]
    df = df.filter(col("Work_Type").isin(valid_types))
    logger.info(f"[VALID] Regle 4 - Work_Type valide : {df.count()} lignes")

    # Regle 5 - Job_Title non vide
    df = df.filter(col("Job_Title").isNotNull() & (trim(col("Job_Title")) != ""))
    logger.info(f"[VALID] Regle 5 - Job_Title non vide : {df.count()} lignes")

    final = df.count()
    logger.info(f"[VALID] Validation OK : {total} -> {final} lignes")
    return df


def validate_dares(df, logger):
    logger.info("[VALID] Validation DARES")

    # Supprimer lignes sans nb_demandeurs
    df = df.filter(col("nb_demandeurs").isNotNull() & (trim(col("nb_demandeurs")) != ""))
    # Garder seulement categorie A (chomeurs stricts)
    df = df.filter(col("categorie") == "A")
    # Garder seulement Total sexe
    df = df.filter(col("sexe") == "Total")
    logger.info(f"[VALID] DARES valides : {df.count()} lignes")
    return df


# -------------------------------------------------
# 6. CACHE + JOINTURE
# -------------------------------------------------
def join_datasets(df_jobs, df_dares, logger):
    logger.info("[JOIN] Mise en cache des DataFrames")

    # CACHE - visible dans Spark UI onglet Storage
    df_jobs.cache()
    df_dares.cache()

    logger.info(f"[JOIN] Jobs en cache : {df_jobs.count()} lignes")
    logger.info(f"[JOIN] DARES en cache : {df_dares.count()} lignes")

    # Normalisation cle de jointure
    df_jobs  = df_jobs.withColumn("join_country",  upper(trim(col("Country"))))
    df_dares = df_dares.withColumn("join_country", upper(trim(col("champ"))))

    logger.info("[JOIN] Jointure offres emploi <-> chomage DARES")
    df_joined = df_jobs.join(
        df_dares.select(
            col("join_country"),
            col("nb_demandeurs").cast("double").alias("nb_demandeurs"),
            col("date_periode"),
            col("tranche_age")
        ),
        on="join_country",
        how="left"
    )

    nb = df_joined.count()
    logger.info(f"[JOIN] Resultat jointure : {nb} lignes")
    return df_joined


# -------------------------------------------------
# 7. AGREGATIONS + WINDOW FUNCTION
# -------------------------------------------------
def aggregate(df, logger):
    logger.info("[AGG] Calcul agregations et window functions")

    # Agregation - nb offres par Job_Title et Country
    df_agg = df.groupBy("Job_Title", "Country", "Work_Type").agg(
        count("*").alias("nb_offres"),
        avg("nb_demandeurs").alias("avg_demandeurs")
    )

    # Window function - RANK par Country
    window_spec = Window.partitionBy("Country").orderBy(col("nb_offres").desc())
    df_agg = df_agg.withColumn("rank_in_country", rank().over(window_spec))

    logger.info(f"[AGG] Agregation terminee : {df_agg.count()} lignes")
    logger.info("[AGG] Top 5 metiers qui recrutent le plus :")

    df_agg.filter(col("rank_in_country") <= 5) \
          .orderBy("Country", "rank_in_country") \
          .show(20, truncate=False)

    return df_agg


# -------------------------------------------------
# 8. ECRITURE SILVER + HIVE
# -------------------------------------------------
def write_silver(df_joined, df_agg, silver_path, spark, logger):
    now   = datetime.now()
    year  = now.strftime("%Y")
    month = now.strftime("%m")
    day   = now.strftime("%d")

    # Ecriture silver - donnees jointes
    out_joined = f"{silver_path}/offres_jointes/year={year}/month={month}/day={day}"
    logger.info(f"[WRITE] Ecriture silver joined -> {out_joined}")
    (df_joined.write
     .mode("overwrite")
     .parquet(out_joined))
    logger.info(f"[WRITE] Silver joined OK : {df_joined.count()} lignes")

    # Ecriture silver - agregats
    out_agg = f"{silver_path}/offres_agregees/year={year}/month={month}/day={day}"
    logger.info(f"[WRITE] Ecriture silver agregats -> {out_agg}")
    (df_agg.write
     .mode("overwrite")
     .parquet(out_agg))
    logger.info(f"[WRITE] Silver agregats OK : {df_agg.count()} lignes")


# -------------------------------------------------
# 9. MAIN
# -------------------------------------------------
def main():
    args = parse_args()

    logger = setup_logger(args.log_path)
    logger.info("=" * 55)
    logger.info("PROCESSOR - Debut traitement Silver")
    logger.info(f"RAW    : {args.raw_path}")
    logger.info(f"SILVER : {args.silver_path}")
    logger.info("=" * 55)

    spark = create_spark_session()
    logger.info(f"Spark version : {spark.version}")

    try:
        # Lecture
        df_jobs, df_dares = read_raw(spark, args.raw_path, logger)

        # Validation
        df_jobs  = validate_jobs(df_jobs, logger)
        df_dares = validate_dares(df_dares, logger)

        # Jointure avec cache
        df_joined = join_datasets(df_jobs, df_dares, logger)

        # Agregations + window function
        df_agg = aggregate(df_joined, logger)

        # Ecriture silver + Hive
        write_silver(df_joined, df_agg, args.silver_path, spark, logger)

        logger.info("=" * 55)
        logger.info("PROCESSOR - Traitement Silver termine avec succes !")
        logger.info("=" * 55)

    except Exception as e:
        logger.error(f"PROCESSOR - ECHEC : {str(e)}")
        raise
    finally:
        spark.stop()
        logger.info("Spark session fermee.")


if __name__ == "__main__":
    main()