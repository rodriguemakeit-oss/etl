import json
import pandas as pd
from pyspark.sql import functions as F

from src.delta_to_rds import config
from src.delta_to_rds.logger import log


def pandas_none(x):
    return None if pd.isna(x) else x


def clean_source_id_str(colname: str):
    return F.regexp_replace(F.col(colname).cast("string"), " ", "").cast("string")


def load_last_version(table_name: str) -> int:
    try:
        txt = config.dbutils.fs.head(config.state_path(table_name), 10000)
        return int(json.loads(txt)["last_version"])
    except Exception:
        return -1


def save_last_version(table_name: str, last_version: int):
    config.dbutils.fs.mkdirs(config.STATE_BASE)
    payload = json.dumps({"last_version": int(last_version)})
    config.dbutils.fs.put(config.state_path(table_name), payload, overwrite=True)


def table_exists_uc(full_name: str) -> bool:
    try:
        return config.spark.catalog.tableExists(full_name)
    except Exception:
        try:
            config.spark.table(full_name).limit(1).count()
            return True
        except Exception:
            return False


def latest_delta_version(uc_table: str) -> int:
    h = config.spark.sql(f"DESCRIBE HISTORY {uc_table} LIMIT 1").collect()
    return int(h[0]["version"])


def uc_object_type(full_name: str) -> str:
    try:
        t = config.spark.catalog.getTable(full_name)
        return "view" if str(t.tableType).upper() == "VIEW" else "table"
    except Exception:
        return "unknown"


def is_delta_table(full_name: str) -> bool:
    try:
        d = config.spark.sql(f"DESCRIBE DETAIL {full_name}").collect()[0].asDict()
        return str(d.get("format", "")).lower() == "delta"
    except Exception:
        return False


def latest_uc_version_or_fallback(full_name: str) -> int:
    if is_delta_table(full_name):
        return latest_delta_version(full_name)
    return -999
