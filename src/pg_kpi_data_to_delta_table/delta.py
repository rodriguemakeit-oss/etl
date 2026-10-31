from pyspark.sql import functions as F
from pyspark.sql import types as T
from src.pg_kpi_data_to_delta_table import config


def qname(full_name):
    return ".".join([f"`{p}`" for p in full_name.split(".")])


def init_watermark_table():
    config.spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {qname(config.WATERMARK_TABLE)} (
      source_table STRING,
      watermark_ts TIMESTAMP,
      watermark_pk STRING
    )
    USING delta
    """)


def get_watermark(source_table):
    rows = (config.spark.table(config.WATERMARK_TABLE)
            .where(F.col("source_table") == source_table)
            .select("watermark_ts", "watermark_pk")
            .take(1))
    if len(rows) == 0:
        return ("1970-01-01 00:00:00", None)
    r = rows[0]
    return (r["watermark_ts"].strftime("%Y-%m-%d %H:%M:%S"), r["watermark_pk"])


def set_watermark(source_table, new_ts_str, new_pk):
    new_pk_sql = "NULL" if new_pk is None else f"'{new_pk}'"
    config.spark.sql(f"""
    MERGE INTO {qname(config.WATERMARK_TABLE)} t
    USING (
      SELECT
        '{source_table}' AS source_table,
        TIMESTAMP('{new_ts_str}') AS watermark_ts,
        {new_pk_sql} AS watermark_pk
    ) s
    ON t.source_table = s.source_table
    WHEN MATCHED THEN UPDATE SET watermark_ts = s.watermark_ts, watermark_pk = s.watermark_pk
    WHEN NOT MATCHED THEN INSERT (source_table, watermark_ts, watermark_pk)
    VALUES (s.source_table, s.watermark_ts, s.watermark_pk)
    """)


def spark_type_to_sql(dt):
    if isinstance(dt, T.StringType): return "STRING"
    if isinstance(dt, T.IntegerType): return "INT"
    if isinstance(dt, T.LongType): return "BIGINT"
    if isinstance(dt, T.DoubleType): return "DOUBLE"
    if isinstance(dt, T.FloatType): return "FLOAT"
    if isinstance(dt, T.BooleanType): return "BOOLEAN"
    if isinstance(dt, T.TimestampType): return "TIMESTAMP"
    if isinstance(dt, T.DateType): return "DATE"
    if isinstance(dt, T.DecimalType): return f"DECIMAL({dt.precision},{dt.scale})"
    return dt.simpleString().upper()


def ensure_delta_table_exists(delta_table, df_schema_source):
    tmp_view = "__tmp_schema_view__"
    df_schema_source.limit(0).createOrReplaceTempView(tmp_view)
    config.spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {qname(delta_table)}
    USING delta
    AS SELECT * FROM {tmp_view}
    """)
    config.spark.catalog.dropTempView(tmp_view)


def ensure_delta_schema_has_columns(delta_table, src_df):
    tgt_cols = set(config.spark.table(delta_table).columns)
    missing_fields = [f for f in src_df.schema.fields if f.name not in tgt_cols]
    if not missing_fields:
        return

    add_cols_sql = ", ".join([f"`{f.name}` {spark_type_to_sql(f.dataType)}" for f in missing_fields])
    print(f"Altering {delta_table}: adding columns -> {[f.name for f in missing_fields]}")
    config.spark.sql(f"ALTER TABLE {qname(delta_table)} ADD COLUMNS ({add_cols_sql})")


def merge_into(delta_table, src_df, pk_col):
    src_df.createOrReplaceTempView("src_changes")
    cols = src_df.columns
    update_cols = [c for c in cols if c != pk_col]
    set_clause = ",\n      ".join([f"t.`{c}` = s.`{c}`" for c in update_cols])
    insert_cols = ", ".join([f"`{c}`" for c in cols])
    insert_vals = ", ".join([f"s.`{c}`" for c in cols])

    config.spark.sql(f"""
    MERGE INTO {qname(delta_table)} t
    USING src_changes s
    ON t.`{pk_col}` = s.`{pk_col}`
    WHEN MATCHED THEN UPDATE SET
      {set_clause}
    WHEN NOT MATCHED THEN INSERT ({insert_cols})
    VALUES ({insert_vals})
    """)
