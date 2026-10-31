import pandas as pd
from pyspark.sql import functions as F

from src.delta_to_rds import config
from src.delta_to_rds.logger import log
from src.delta_to_rds.uc.utils import (
    clean_source_id_str, pandas_none, load_last_version,
    save_last_version, table_exists_uc, latest_uc_version_or_fallback,
)
from src.delta_to_rds.uc.cdf import read_cdf_batch
from src.delta_to_rds.pg.helpers import upsert_rows, upsert_rows_on_cols, soft_delete
from src.delta_to_rds.pg.maps import pg_map_source_to_uuid, pg_ddv_lookup
from src.delta_to_rds.pg.snapshot import snapshot_df, snapshot_reconcile_deletes


def parse_month_any(colname: str):
    c = F.trim(F.col(colname).cast("string"))
    c_norm = F.lower(c)
    c_norm = F.regexp_replace(c_norm, r"janv", "jan")
    c_norm = F.regexp_replace(c_norm, r"févr|fevr", "feb")
    c_norm = F.regexp_replace(c_norm, r"mars", "mar")
    c_norm = F.regexp_replace(c_norm, r"avr", "apr")
    c_norm = F.regexp_replace(c_norm, r"mai", "may")
    c_norm = F.regexp_replace(c_norm, r"juin", "jun")
    c_norm = F.regexp_replace(c_norm, r"juil", "jul")
    c_norm = F.regexp_replace(c_norm, r"août|aout", "aug")
    c_norm = F.regexp_replace(c_norm, r"sept", "sep")
    c_norm = F.regexp_replace(c_norm, r"déc|dec", "dec")

    return F.coalesce(
        F.try_to_timestamp(F.concat(F.lit("01/"), c_norm), F.lit("dd/MM/yyyy")),
        F.try_to_timestamp(F.concat(c_norm, F.lit("-01")), F.lit("yyyy-MM-dd")),
        F.try_to_timestamp(F.concat(F.lit("01/"), c_norm), F.lit("dd/MMM-yy")),
        F.try_to_timestamp(F.concat(F.lit("01/"), c_norm), F.lit("dd/MMM-yyyy")),
        F.try_to_timestamp(c_norm)
    )


def sync_data_domain_version_from_exercise():
    uc_name = "version_scenario_life_time_wapp"
    state_name = "data_domain_version__derived"
    uc = config.uc_tbl(uc_name)
    ctx = {"uc_table": uc, "uc_name": uc_name, "pg_table": "data_domain_version", "state_name": state_name}

    if not table_exists_uc(uc):
        log("UC_TABLE_MISSING_SKIP_DDV", **ctx)
        return {"table": state_name, "processed": False, "mode": "skip_missing", "skipped_fk": 0, "upserts": 0, "deletes": 0}

    last = load_last_version(state_name)
    latest = latest_uc_version_or_fallback(uc)
    log("DDV_DERIVE_START", last=last, latest=latest, **ctx)

    def pairs_from_df(df):
        return (df
            .withColumn("dd_source_id", clean_source_id_str("data_domain_id"))
            .withColumn("v_source_id", clean_source_id_str("version_id"))
            .select("dd_source_id", "v_source_id")
            .dropna()
            .dropDuplicates(["dd_source_id", "v_source_id"])
        )

    if last < 0:
        mode = "snapshot"
        pairs = pairs_from_df(snapshot_df(uc))
    else:
        start = last + 1
        if latest == -999:
            mode = "snapshot_view"
            pairs = pairs_from_df(snapshot_df(uc))
        else:
            if start > latest:
                log("DDV_DERIVE_NOOP", reason="start>latest", **ctx)
                return {"table": state_name, "processed": False, "mode": "noop", "skipped_fk": 0, "upserts": 0, "deletes": 0}
            cdf = read_cdf_batch(uc, start, latest)
            if cdf is None:
                mode = "snapshot_fallback"
                pairs = pairs_from_df(snapshot_df(uc))
            else:
                mode = "cdf"
                up = cdf.filter(F.col("_change_type").isin("insert", "update_postimage"))
                pairs = pairs_from_df(up)

    dd_map = pg_map_source_to_uuid("data_domain", "dd")
    v_map = pg_map_source_to_uuid("version", "v")

    total = pairs.count()
    resolved = (pairs
        .join(dd_map, pairs["dd_source_id"] == dd_map["dd_source_id"], "inner")
        .join(v_map, pairs["v_source_id"] == v_map["v_source_id"], "inner")
        .select(
            F.expr("uuid()").alias("id"),
            F.col("dd_uuid").alias("data_domain_id"),
            F.col("v_uuid").alias("version_id"),
        )
    )

    ok = resolved.count()
    skipped_fk = max(0, total - ok)
    pdf = resolved.toPandas()

    rows = []
    for r in pdf.itertuples(index=False):
        rows.append((pandas_none(r.id), pandas_none(r.data_domain_id), pandas_none(r.version_id), "{}"))

    upserts = upsert_rows_on_cols(
        "data_domain_version",
        cols=["id", "data_domain_id", "version_id", "forbidden_unit_status"],
        rows=rows,
        conflict_cols=["data_domain_id", "version_id"],
        reactivate=False,
        context={**ctx, "mode": mode},
        exclude_update_cols=["forbidden_unit_status"],
    )

    save_last_version(state_name, (last + 1) if latest == -999 else latest)
    log("DDV_DERIVE_OK", mode=mode, inserted=len(rows), skipped_fk=skipped_fk, **ctx)
    return {"table": state_name, "processed": True, "mode": mode, "skipped_fk": skipped_fk, "upserts": len(rows), "deletes": 0}


EXERCISE_COLS = [
    "id", "code", "description", "start_year", "end_year",
    "month_start", "month_end", "is_active", "scenario_id",
    "data_domain_version_id", "source_id",
]


def sync_exercise():
    uc_name = "version_scenario_life_time_wapp"
    pg_name = "exercise"
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
        base_df = (base_df
            .withColumn("scenario_source_id", clean_source_id_str("scenario_id"))
            .withColumn("dd_source_id", clean_source_id_str("data_domain_id"))
            .withColumn("v_source_id", clean_source_id_str("version_id"))
            .withColumn("start_year", F.col("start_year").cast("int"))
            .withColumn("end_year", F.col("end_year").cast("int"))
            .withColumn("month_start", parse_month_any("month_start"))
            .withColumn("month_end", parse_month_any("month_end"))
            .withColumn("is_active", F.lit(False).cast("boolean"))
        )

        scenario_map = pg_map_source_to_uuid("scenario", "sc")
        ddv_lu = pg_ddv_lookup()
        total = base_df.count()

        joined = (base_df
            .join(scenario_map, base_df["scenario_source_id"] == scenario_map["sc_source_id"], "inner")
            .join(ddv_lu, on=["dd_source_id", "v_source_id"], how="inner")
            .withColumn("id", F.expr("uuid()"))
            .select(
                F.col("id").alias("id"),
                F.col("code").cast("string").alias("code"),
                F.col("description").cast("string").alias("description"),
                F.col("start_year").alias("start_year"),
                F.col("end_year").alias("end_year"),
                F.col("month_start").alias("month_start"),
                F.col("month_end").alias("month_end"),
                F.col("is_active").alias("is_active"),
                F.col("sc_uuid").alias("scenario_id"),
                F.col("ddv_uuid").alias("data_domain_version_id"),
                F.col("source_id").alias("source_id"),
            )
        )

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
            rows.append((
                pandas_none(r.id), pandas_none(r.code), pandas_none(r.description),
                None if pd.isna(r.start_year) else int(r.start_year),
                None if pd.isna(r.end_year) else int(r.end_year),
                None if pd.isna(r.month_start) else r.month_start,
                None if pd.isna(r.month_end) else r.month_end,
                False, pandas_none(r.scenario_id), pandas_none(r.data_domain_version_id), sid
            ))
        return rows, snapshot_ids

    def _do_snapshot(mode, save_version):
        joined, skipped_fk = transform(snapshot_df(uc))
        rows, snapshot_ids = _to_rows(joined.toPandas())
        upserts = upsert_rows(
            pg_name, EXERCISE_COLS, rows, conflict_col="source_id",
            reactivate=True, context={**ctx, "mode": mode}, exclude_update_cols=["is_active"],
        )
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

    upserts = upsert_rows(pg_name, EXERCISE_COLS, rows, conflict_col="source_id", reactivate=True, context={**ctx, "mode": mode}, exclude_update_cols=["is_active"])
    save_last_version(uc_name, latest)
    log("SYNC_OK", mode=mode, upserts=upserts, deletes=deletes, skipped_fk=skipped_fk, **ctx)
    return {"table": uc_name, "processed": True, "mode": mode, "skipped_fk": skipped_fk, "upserts": upserts, "deletes": deletes}
