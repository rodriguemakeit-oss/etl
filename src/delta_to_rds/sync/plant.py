import pandas as pd
from pyspark.sql import functions as F

from src.delta_to_rds import config
from src.delta_to_rds.logger import log
from src.delta_to_rds.uc.utils import (
    clean_source_id_str, pandas_none, load_last_version,
    save_last_version, table_exists_uc, latest_uc_version_or_fallback,
)
from src.delta_to_rds.uc.cdf import read_cdf_batch
from src.delta_to_rds.pg.helpers import upsert_rows, soft_delete
from src.delta_to_rds.pg.maps import pg_map_source_to_uuid
from src.delta_to_rds.pg.snapshot import snapshot_df, snapshot_reconcile_deletes


COLS = ["id", "code", "description", "country_id", "source_id"]


def sync_plant():
    uc_name = "plant_wapp"
    pg_name = "plant"
    uc = config.uc_tbl(uc_name)
    ctx = {"uc_table": uc, "uc_name": uc_name, "pg_table": pg_name}

    if not table_exists_uc(uc):
        log("UC_TABLE_MISSING", **ctx)
        return {"table": uc_name, "processed": False, "mode": "skip_missing", "skipped_fk": 0, "upserts": 0, "deletes": 0}

    last = load_last_version(uc_name)
    latest = latest_uc_version_or_fallback(uc)
    log("SYNC_START", last=last, latest=latest, **ctx)

    def transform(base_df):
        base_df = base_df.withColumn("source_id", clean_source_id_str("id"))
        base_df = base_df.withColumn("country_source_id", clean_source_id_str("country_id"))
        country_map = pg_map_source_to_uuid("country", "country")

        total = base_df.count()
        joined = (base_df.join(country_map, base_df["country_source_id"] == country_map["country_source_id"], "inner")
                        .withColumn("id", F.expr("uuid()"))
                        .select(
                            F.col("id").alias("id"),
                            F.col("code").cast("string").alias("code"),
                            F.col("description").cast("string").alias("description"),
                            F.col("country_uuid").alias("country_id"),
                            F.col("source_id").alias("source_id"),
                        ))
        resolved = joined.count()
        return joined, max(0, total - resolved)

    def _to_rows(pdf):
        rows = []
        snapshot_ids = set()
        for r in pdf.itertuples(index=False):
            sid = None if pd.isna(r.source_id) else str(r.source_id)
            if sid is None:
                continue
            snapshot_ids.add(sid)
            rows.append((pandas_none(r.id), pandas_none(r.code), pandas_none(r.description), pandas_none(r.country_id), sid))
        return rows, snapshot_ids

    def _do_snapshot(mode, save_version):
        joined, skipped_fk = transform(snapshot_df(uc))
        rows, snapshot_ids = _to_rows(joined.toPandas())
        upserts = upsert_rows(pg_name, COLS, rows, conflict_col="source_id", reactivate=True, context={**ctx, "mode": mode})
        deletes = 0
        if config.FULL_RECONCILE_DELETES_ON_SNAPSHOT:
            deletes = snapshot_reconcile_deletes(pg_name, snapshot_ids, context={**ctx, "mode": mode})
        save_last_version(uc_name, save_version)
        log("SYNC_OK", mode=mode, upserts=upserts, deletes=deletes, skipped_fk=skipped_fk, **ctx)
        return {"table": uc_name, "processed": True, "mode": mode, "skipped_fk": skipped_fk, "upserts": upserts, "deletes": deletes}

    if last < 0:
        return _do_snapshot("snapshot", latest if latest != -999 else 0)

    if latest == -999:
        return _do_snapshot("snapshot_view", last + 1)

    start = last + 1
    if start > latest:
        log("SYNC_NOOP", reason="start>latest", **ctx)
        return {"table": uc_name, "processed": False, "mode": "noop", "skipped_fk": 0, "upserts": 0, "deletes": 0}

    cdf = read_cdf_batch(uc, start, latest)
    if cdf is None:
        return _do_snapshot("snapshot_fallback", latest)

    mode = "cdf"
    cdf = cdf.withColumn("source_id", clean_source_id_str("id"))

    del_ids = (cdf.filter(F.col("_change_type") == "delete")
                 .select("source_id").distinct()
                 .toPandas()["source_id"].dropna().astype(str).tolist())
    deletes = soft_delete(pg_name, del_ids, context={**ctx, "mode": mode})

    up = cdf.filter(F.col("_change_type").isin("insert", "update_postimage"))
    joined, skipped_fk = transform(up)
    rows, _ = _to_rows(joined.toPandas())

    upserts = upsert_rows(pg_name, COLS, rows, conflict_col="source_id", reactivate=True, context={**ctx, "mode": mode})
    save_last_version(uc_name, latest)
    log("SYNC_OK", mode=mode, upserts=upserts, deletes=deletes, skipped_fk=skipped_fk, **ctx)
    return {"table": uc_name, "processed": True, "mode": mode, "skipped_fk": skipped_fk, "upserts": upserts, "deletes": deletes}
