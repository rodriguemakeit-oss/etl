from pyspark.sql import functions as F

from src.delta_to_rds import config


def pg_map_source_to_uuid(pg_table: str, alias: str):
    df = config.spark.read.jdbc(url=config.jdbc_url, table=pg_table, properties=config.jdbc_props)
    return df.select(
        F.col("source_id").cast("string").alias(f"{alias}_source_id"),
        F.col("id").alias(f"{alias}_uuid")
    )


def pg_ddv_lookup():
    ddv = config.spark.read.jdbc(url=config.jdbc_url, table="data_domain_version", properties=config.jdbc_props).select(
        F.col("id").alias("ddv_uuid"),
        F.col("data_domain_id").alias("dd_uuid"),
        F.col("version_id").alias("v_uuid")
    )

    dd = config.spark.read.jdbc(url=config.jdbc_url, table="data_domain", properties=config.jdbc_props).select(
        F.col("id").alias("dd_uuid"),
        F.col("source_id").cast("string").alias("dd_source_id")
    )

    v = config.spark.read.jdbc(url=config.jdbc_url, table="version", properties=config.jdbc_props).select(
        F.col("id").alias("v_uuid"),
        F.col("source_id").cast("string").alias("v_source_id")
    )

    return (ddv.join(dd, "dd_uuid", "inner")
           .join(v, "v_uuid", "inner")
           .select("dd_source_id", "v_source_id", "ddv_uuid")
           .groupBy("dd_source_id", "v_source_id")
           .agg(F.min("ddv_uuid").alias("ddv_uuid")))
