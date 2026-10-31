from pyspark.sql import functions as F
from src.pg_kpi_data_to_delta_table import config
from src.pg_kpi_data_to_delta_table.delta import (
    init_watermark_table,
    get_watermark,
    set_watermark,
    ensure_delta_table_exists,
    ensure_delta_schema_has_columns,
    merge_into,
)
from src.pg_kpi_data_to_delta_table.enrich import (
    enrich_user_cols,
    enrich_simple_user_id,
    enrich_exercise,
    enrich_kpi_log,
    enrich_fk_source_ids,
)


def df_is_empty(df):
    return len(df.take(1)) == 0


def sync_all():
    init_watermark_table()

    for t in config.TABLES:
        pg_table = t["pg_table"]
        delta_table = t["delta_table"]
        pk_col = t["pk_col"]
        created_col = t["created_col"]
        updated_col = t["updated_col"]
        fk_mappings = t.get("fk_mappings", [])

        last_ts, last_pk = get_watermark(pg_table)

        pk_tie_clause = "TRUE" if last_pk is None else f"{pk_col} > '{last_pk}'"
        event_expr = f"COALESCE({updated_col}, {created_col}, TIMESTAMP '1970-01-01 00:00:00')"

        query = f"""
        (SELECT
            *,
            {event_expr} AS _event_time
        FROM {pg_table}
        WHERE
          ({event_expr} > TIMESTAMP '{last_ts}')
          OR
          ({event_expr} = TIMESTAMP '{last_ts}' AND {pk_tie_clause})
        ) AS q
        """

        inc_df = config.spark.read.jdbc(config.DB_URL, query, properties=config.DB_PROPS)

        if df_is_empty(inc_df):
            print(f"[{pg_table}] No changes since ({last_ts}, {last_pk})")
            continue

        inc_df = inc_df.withColumn("_ingested_at", F.current_timestamp())

        user_cols = [c for c in ["created_by_user_id", "updated_by_user_id"] if c in inc_df.columns]
        inc_df = enrich_user_cols(inc_df, user_cols)

        if pg_table == "public.kpi_data":
            inc_df = enrich_exercise(inc_df)

        inc_df = enrich_simple_user_id(inc_df)

        if pg_table == "public.kpi_log":
            if "user_firstname" in inc_df.columns:
                inc_df = inc_df.withColumnRenamed("user_firstname", "firstname")
            if "user_lastname" in inc_df.columns:
                inc_df = inc_df.withColumnRenamed("user_lastname", "name")
            inc_df = enrich_kpi_log(inc_df)

        inc_df = enrich_fk_source_ids(inc_df, fk_mappings)

        lk_cols = [c for c in inc_df.columns if c.startswith("lk_")]
        if lk_cols:
            inc_df = inc_df.drop(*lk_cols)

        ensure_delta_table_exists(delta_table, inc_df.drop("_event_time"))
        ensure_delta_schema_has_columns(delta_table, inc_df.drop("_event_time"))

        inc_dedup = (inc_df
                     .orderBy(F.col("_event_time").desc(), F.col(pk_col).desc())
                     .dropDuplicates([pk_col])
                     .drop("_event_time"))

        merge_into(delta_table, inc_dedup, pk_col)

        last_row = (inc_df
                    .select(F.col("_event_time").alias("et"), F.col(pk_col).alias("pk"))
                    .orderBy(F.col("et").desc(), F.col("pk").desc())
                    .limit(1)
                    .collect()[0])

        new_ts_str = last_row["et"].strftime("%Y-%m-%d %H:%M:%S")
        new_pk = last_row["pk"]
        set_watermark(pg_table, new_ts_str, new_pk)

        print(f"[{pg_table}] MERGED {inc_dedup.count()} rows into {delta_table} | watermark: ({new_ts_str}, {new_pk})")
