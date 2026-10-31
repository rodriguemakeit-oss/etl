from pyspark.sql import functions as F
from src.pg_kpi_data_to_delta_table.lookups import (
    read_lookup,
    read_user_gid_lookup,
    read_exercise_enrich_lookup,
    read_kpi_data_enrich_lookup,
    read_kpi_comment_latest_lookup,
)


def enrich_user_cols(df, user_cols):
    if not user_cols:
        return df

    user_lk = read_user_gid_lookup()

    if "created_by_user_id" in user_cols:
        cb = (user_lk
              .withColumnRenamed("lk_user_id", "lk_cb_uuid")
              .withColumnRenamed("lk_user_gid", "lk_cb_gid")
              .withColumnRenamed("lk_user_firstname", "lk_cb_firstname")
              .withColumnRenamed("lk_user_lastname", "lk_cb_lastname"))

        df = (df
            .join(F.broadcast(cb), df["created_by_user_id"] == F.col("lk_cb_uuid"), "left")
            .withColumn("created_by_user_id", F.coalesce(F.col("lk_cb_gid"), F.col("created_by_user_id")))
            .withColumn("created_by_firstname", F.col("lk_cb_firstname"))
            .withColumn("created_by_lastname", F.col("lk_cb_lastname"))
            .drop("lk_cb_uuid", "lk_cb_gid", "lk_cb_firstname", "lk_cb_lastname")
        )

    if "updated_by_user_id" in user_cols:
        ub = (user_lk
              .withColumnRenamed("lk_user_id", "lk_ub_uuid")
              .withColumnRenamed("lk_user_gid", "lk_ub_gid")
              .withColumnRenamed("lk_user_firstname", "lk_ub_firstname")
              .withColumnRenamed("lk_user_lastname", "lk_ub_lastname"))

        df = (df
            .join(F.broadcast(ub), df["updated_by_user_id"] == F.col("lk_ub_uuid"), "left")
            .withColumn("updated_by_user_id", F.coalesce(F.col("lk_ub_gid"), F.col("updated_by_user_id")))
            .withColumn("updated_by_firstname", F.col("lk_ub_firstname"))
            .withColumn("updated_by_lastname", F.col("lk_ub_lastname"))
            .drop("lk_ub_uuid", "lk_ub_gid", "lk_ub_firstname", "lk_ub_lastname")
        )

    return df


def enrich_simple_user_id(df):
    if "user_id" not in df.columns:
        return df

    user_lk = read_user_gid_lookup()
    u = (user_lk
        .withColumnRenamed("lk_user_id", "lk_uuid")
        .withColumnRenamed("lk_user_gid", "lk_gid")
        .withColumnRenamed("lk_user_firstname", "lk_firstname")
        .withColumnRenamed("lk_user_lastname", "lk_lastname"))

    df = (df
        .join(F.broadcast(u), df["user_id"] == F.col("lk_uuid"), "left")
        .withColumn("user_id", F.coalesce(F.col("lk_gid"), F.col("user_id")))
        .withColumn("user_firstname", F.col("lk_firstname"))
        .withColumn("user_lastname", F.col("lk_lastname"))
        .drop("lk_uuid", "lk_gid", "lk_firstname", "lk_lastname")
    )
    return df


def enrich_exercise(df):
    if "exercise_id" not in df.columns:
        return df

    ex_lk = read_exercise_enrich_lookup()
    df = (df
        .join(F.broadcast(ex_lk), df["exercise_id"] == ex_lk["lk_exercise_id"], "left")
        .withColumn("exercise_source_id", F.col("lk_exercise_source_id"))
        .withColumn("exercise_scenario_id", F.col("lk_scenario_id"))
        .withColumn("data_domain_source_id", F.col("lk_data_domain_source_id"))
        .drop("lk_exercise_id", "lk_exercise_source_id", "lk_scenario_id", "lk_data_domain_source_id")
    )
    return df


def enrich_kpi_log(df):
    if "kpi_data_id" not in df.columns:
        return df

    kd_lk = read_kpi_data_enrich_lookup()
    kc_lk = read_kpi_comment_latest_lookup()

    df = (df
        .join(F.broadcast(kd_lk), df["kpi_data_id"] == kd_lk["lk_kpi_data_id"], "left")
        .withColumn("exercise_source_id", F.col("lk_exercise_source_id"))
        .withColumn("exercise_scenario_id", F.col("lk_exercise_scenario_id"))
        .withColumn("data_domain_source_id", F.col("lk_data_domain_source_id"))
        .withColumn("signal_source_id", F.col("lk_signal_source_id"))
        .withColumn("unit_source_id", F.col("lk_unit_source_id"))
        .withColumn("version_source_id", F.col("lk_version_source_id"))
        .withColumn("granularity", F.col("lk_granularity"))
        .withColumn("period_year", F.col("lk_period_year"))
        .withColumn("period_month", F.col("lk_period_month"))
        .drop(
            "lk_kpi_data_id",
            "lk_exercise_source_id", "lk_exercise_scenario_id", "lk_data_domain_source_id",
            "lk_signal_source_id", "lk_unit_source_id", "lk_version_source_id",
            "lk_granularity", "lk_period_year", "lk_period_month",
        )
    )

    df = (df
        .join(F.broadcast(kc_lk), df["kpi_data_id"] == kc_lk["lk_kpi_data_id"], "left")
        .withColumn("comment", F.col("lk_comment"))
        .drop("lk_kpi_data_id", "lk_comment")
    )
    return df


def enrich_fk_source_ids(df, fk_mappings):
    for fk_col, lk_table, lk_pk, lk_source_col, out_col in fk_mappings:
        if fk_col not in df.columns:
            continue
        lk_df = read_lookup(lk_table, lk_pk, lk_source_col)
        df = (df
              .join(F.broadcast(lk_df), df[fk_col] == lk_df["lk_id"], "left")
              .withColumn(out_col, F.col("lk_source_id"))
              .drop("lk_id", "lk_source_id"))
    return df
