# -*- coding: utf-8 -*-
import argparse
import logging
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, round as spark_round

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver_path",    required=True)
    parser.add_argument("--mysql_host",     required=True)
    parser.add_argument("--mysql_db",       required=True)
    parser.add_argument("--mysql_user",     required=True)
    parser.add_argument("--mysql_password", required=True)
    parser.add_argument("--log_path",       required=True)
    return parser.parse_args()

def setup_logger(log_path):
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    logger = logging.getLogger("datamart")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    return logger

def create_spark_session():
    return SparkSession.builder.appName("Datamart_Emploi_France").config("spark.jars", "/source/mysql-connector-java-8.0.28.jar").getOrCreate()

def write_mysql(df, table, url, user, pwd, logger):
    logger.info(f"[MYSQL] Ecriture -> {table} ({df.count()} lignes)")
    df.write.format("jdbc").option("url", url).option("dbtable", table).option("user", user).option("password", pwd).option("driver", "com.mysql.cj.jdbc.Driver").mode("overwrite").save()
    logger.info(f"[MYSQL] {table} OK")

def main():
    args = parse_args()
    logger = setup_logger(args.log_path)
    logger.info("=" * 55)
    logger.info("DATAMART - Debut creation couche Gold")
    logger.info("=" * 55)
    spark = create_spark_session()
    jdbc_url = f"jdbc:mysql://{args.mysql_host}:3306/{args.mysql_db}?useSSL=false&allowPublicKeyRetrieval=true&characterEncoding=UTF-8"
    try:
        # DM1 - Top metiers
        logger.info("[DM1] datamart_top_metiers")
        df_agg = spark.read.parquet(f"{args.silver_path}/offres_agregees/")
        df_agg.cache()
        dm1 = df_agg.filter(col("nb_offres") >= 30).filter(col("rank_in_country") <= 20).select(col("Job_Title").alias("job_title"), col("Country").alias("country"), col("Work_Type").alias("work_type"), col("nb_offres"), spark_round(col("avg_demandeurs"), 2).alias("avg_demandeurs"), col("rank_in_country")).orderBy("country", "rank_in_country")
        write_mysql(dm1, "datamart_top_metiers", jdbc_url, args.mysql_user, args.mysql_password, logger)

        # DM2 - Chomage vs offres
        logger.info("[DM2] datamart_chomage_vs_offres")
        df_joint = spark.read.parquet(f"{args.silver_path}/offres_jointes/")
        df_joint.cache()
        dm2 = df_joint.filter(col("nb_demandeurs").isNotNull()).groupBy("join_country").agg(count("*").alias("nb_offres_total"), spark_round(avg("nb_demandeurs"), 2).alias("avg_demandeurs")).withColumnRenamed("join_country", "country").withColumn("ratio_offres_demandeurs", spark_round(col("nb_offres_total") / (col("avg_demandeurs") + 1), 4)).orderBy(col("nb_offres_total").desc())
        write_mysql(dm2, "datamart_chomage_vs_offres", jdbc_url, args.mysql_user, args.mysql_password, logger)

        # DM3 - Profils demandes
        logger.info("[DM3] datamart_profils_demandes")
        dm3 = df_agg.groupBy("Work_Type").agg(count("*").alias("nb_combinaisons"), spark_round(avg("nb_offres"), 2).alias("avg_offres_par_metier"), spark_round(avg("rank_in_country"), 2).alias("avg_rank")).withColumnRenamed("Work_Type", "work_type").orderBy(col("avg_offres_par_metier").desc())
        write_mysql(dm3, "datamart_profils_demandes", jdbc_url, args.mysql_user, args.mysql_password, logger)

        logger.info("=" * 55)
        logger.info("DATAMART - Couche Gold creee avec succes !")
        logger.info("=" * 55)
    except Exception as e:
        logger.error(f"DATAMART - ECHEC : {str(e)}")
        raise
    finally:
        spark.stop()
        logger.info("Spark session fermee.")

if __name__ == "__main__":
    main()
