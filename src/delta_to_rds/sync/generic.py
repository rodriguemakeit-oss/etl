import pandas as pd
from pyspark.sql import functions as F

from src.delta_to_rds import config
from src.delta_to_rds.logger import log
from src.delta_to_rds.uc.utils import (
    clean_source_id_str, pandas_none, load_last_version,
    save_last_version, table_exists_uc, latest_uc_version_or_fallback,
)
from src.delta_to_rds.uc.cdf import read_cdf_batch
from src.delta_to_rds.pg.helpers import build_mapped_exprs, upsert_rows, soft_delete
from src.delta_to_rds.pg.snapshot import snapshot_df, snapshot_reconcile_deletes


def sync_dim_generic(uc_name: str, pg_name: str, mapping: dict, fill_missing: dict = None):
    uc = config.uc_tbl(uc_name)
    ctx = {"uc_table": uc, "uc_name": uc_name, "pg_table": pg_name}

    if not table_exists_uc(uc):
        log("UC_TABLE_MISSING", **ctx)
        return {"table": uc_name, "processed": False, "mode": "skip_missing", "skipped_fk": 0, "upserts": 0, "deletes": 0}

    last = load_last_version(uc_name)
    latest = latest_uc_version_or_fallback(uc)
    log("SYNC_START", last=last, latest=latest, **ctx)

    def _build_rows(base_df):
        base_df = base_df.withColumn("source_id", clean_source_id_str("id"))
        exprs = build_mapped_exprs(base_df, pg_name, mapping, fill_missing)

        df_final = base_df.withColumn("id", F.expr("uuid()")).select(
            F.col("id").alias("id"), *exprs, F.col("source_id").alias("source_id")
        )

        pdf = df_final.toPandas()
        cols = ["id"] + list(mapping.keys()) + ["source_id"]

        rows = []
        snapshot_ids = set()
        for r in pdf.itertuples(index=False):
            sid = None if pd.isna(r.source_id) else str(r.source_id)
            if sid is None:
                continue
            snapshot_ids.add(sid)
            row = [pandas_none(r.id)]
            for c in mapping.keys():
                row.append(pandas_none(getattr(r, c)))
            row.append(sid)
            rows.append(tuple(row))

        return cols, rows, snapshot_ids

    def _upsert_and_reconcile(cols, rows, snapshot_ids, mode, save_version):
        upserts = upsert_rows(pg_name, cols, rows, conflict_col="source_id", reactivate=True, context={**ctx, "mode": mode})
        deletes = 0
        if config.FULL_RECONCILE_DELETES_ON_SNAPSHOT:
            deletes = snapshot_reconcile_deletes(pg_name, snapshot_ids, context={**ctx, "mode": mode})
        save_last_version(uc_name, save_version)
        log("SYNC_OK", mode=mode, upserts=upserts, deletes=deletes, **ctx)
        return {"table": uc_name, "processed": True, "mode": mode, "skipped_fk": 0, "upserts": upserts, "deletes": deletes}

    if last < 0:
        cols, rows, snapshot_ids = _build_rows(snapshot_df(uc))
        return _upsert_and_reconcile(cols, rows, snapshot_ids, "snapshot", latest if latest != -999 else 0)

    if latest == -999:
        cols, rows, snapshot_ids = _build_rows(snapshot_df(uc))
        return _upsert_and_reconcile(cols, rows, snapshot_ids, "snapshot_view", last + 1)

    start = last + 1
    if start > latest:
        log("SYNC_NOOP", reason="start>latest", **ctx)
        return {"table": uc_name, "processed": False, "mode": "noop", "skipped_fk": 0, "upserts": 0, "deletes": 0}

    df = read_cdf_batch(uc, start, latest)

    if df is None:
        cols, rows, snapshot_ids = _build_rows(snapshot_df(uc))
        return _upsert_and_reconcile(cols, rows, snapshot_ids, "snapshot_fallback", latest)

    mode = "cdf"
    df = df.withColumn("source_id", clean_source_id_str("id"))

    del_ids = (df.filter(F.col("_change_type") == "delete")
                 .select("source_id").distinct()
                 .toPandas()["source_id"].dropna().astype(str).tolist())
    deletes = soft_delete(pg_name, del_ids, context={**ctx, "mode": mode})

    up = df.filter(F.col("_change_type").isin("insert", "update_postimage"))
    exprs = build_mapped_exprs(up, pg_name, mapping, fill_missing)

    df_final = up.withColumn("id", F.expr("uuid()")).select(
        F.col("id").alias("id"), *exprs, F.col("source_id").alias("source_id")
    )

    pdf = df_final.toPandas()
    cols = ["id"] + list(mapping.keys()) + ["source_id"]

    rows = []
    for r in pdf.itertuples(index=False):
        sid = None if pd.isna(r.source_id) else str(r.source_id)
        if sid is None:
            continue
        row = [pandas_none(r.id)]
        for c in mapping.keys():
            row.append(pandas_none(getattr(r, c)))
        row.append(sid)
        rows.append(tuple(row))

    upserts = upsert_rows(pg_name, cols, rows, conflict_col="source_id", reactivate=True, context={**ctx, "mode": mode})
    save_last_version(uc_name, latest)
    log("SYNC_OK", mode=mode, upserts=upserts, deletes=deletes, **ctx)
    return {"table": uc_name, "processed": True, "mode": mode, "skipped_fk": 0, "upserts": upserts, "deletes": deletes}
