# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Sync UC Delta → PostgreSQL RDS (Event-Based)
# MAGIC - Incremental via Delta CDF when available
# MAGIC - Full snapshot backfill on first run
# MAGIC - Fallback to snapshot if CDF gap detected

# COMMAND ----------

from src.delta_to_rds import config
from src.delta_to_rds.logger import log
from src.delta_to_rds.uc.utils import table_exists_uc
from src.delta_to_rds.sync.router import ALL_UC_TABLES, ORDER, tables_with_changes, run_one_table
from src.delta_to_rds.pg.helpers import pg_execute, pg_fetchone

import json
import traceback
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Initialize config (credentials, catalog, PG connection)

# COMMAND ----------

dbutils.widgets.text("ENV", "dev")
ENV = dbutils.widgets.get("ENV").strip().lower()

config.init(spark, dbutils, env=ENV)

pg_execute("""
CREATE TABLE IF NOT EXISTS public.master_data_sync_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    synced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at TIMESTAMPTZ,
    status VARCHAR(20) NOT NULL DEFAULT 'running',
    triggered_by VARCHAR(255),
    rows_updated INTEGER,
    error_message TEXT,
    tables_detail TEXT
)
""")

try:
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    _job_id = ctx.jobId().getOrElse(None)
    _triggered_by = str(_job_id) if _job_id else "manual"
except Exception:
    _triggered_by = "manual"

_sync_log_id = pg_fetchone(
    "INSERT INTO public.master_data_sync_log (synced_at, status, triggered_by) VALUES (now(), 'running', %s) RETURNING id",
    (_triggered_by,)
)[0]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Detect changes & run sync (multi-pass)

# COMMAND ----------

MAX_PASSES = 3

dbutils.widgets.text("FORCED_TABLES", "")
FORCE_TABLES_RAW = dbutils.widgets.get("FORCED_TABLES").strip()
FORCE_SET = {x.strip() for x in FORCE_TABLES_RAW.split(",") if x.strip()}

all_stats = []
_sync_error = None
_table_errors = {}

try:
    missing_uc_tables = []
    for t in ALL_UC_TABLES:
        full = config.uc_tbl(t)
        if not table_exists_uc(full):
            missing_uc_tables.append(full)

    if missing_uc_tables:
        msg = "Pipeline aborted: missing UC tables:\n" + "\n".join([f"  - {t}" for t in missing_uc_tables])
        raise Exception(msg)

    log("UC_PRECHECK_OK", count=len(ALL_UC_TABLES))

    if FORCE_SET:
        changed_tables = list(FORCE_SET)
    else:
        changed_tables = tables_with_changes()

    if FORCE_SET:
        pending = [t for t in ORDER if t in FORCE_SET]
    else:
        pending = [t for t in ORDER if t in changed_tables or (t == "data_domain_version__derived" and "version_scenario_life_time_wapp" in changed_tables)]

    if not pending and not FORCE_SET:
        log("NOTHING_TO_DO", reason="no changed tables")
    else:
        to_retry = pending[:]

        for p in range(1, MAX_PASSES + 1):
            log("PASS_START", pass_n=p, max_passes=MAX_PASSES, pending=to_retry)
            retry_next = []
            pass_stats = []

            for t in ORDER:
                if FORCE_SET:
                    to_run = (t in FORCE_SET)
                else:
                    to_run = (t in to_retry)

                if not to_run:
                    continue

                log("TABLE_RUN_START", table=t, pass_n=p, forced=bool(FORCE_SET))

                try:
                    stats = run_one_table(t)
                    pass_stats.append(stats)
                    log("TABLE_RUN_OK", table=t, pass_n=p, stats=stats)

                    if stats.get("skipped_fk", 0) > 0 and t in ["plant_wapp", "unit_wapp", "data_domain_version__derived", "version_scenario_life_time_wapp", "signal_life_time_wapp"]:
                        retry_next.append(t)

                except Exception as e:
                    log("TABLE_RUN_FAIL", table=t, pass_n=p, err_type=type(e).__name__, err=str(e))
                    print(traceback.format_exc())
                    _table_errors[t] = str(e)

            all_stats.extend(pass_stats)

            if not retry_next:
                log("PASS_STOP_EARLY", pass_n=p, reason="no FK-skips remaining")
                break

            to_retry = list(dict.fromkeys(retry_next))
            log("PASS_END", pass_n=p, retry_next=to_retry)

        log("SUMMARY_START", rows=len(all_stats))
        if all_stats:
            print(pd.DataFrame(all_stats).to_string())
        if _table_errors:
            log("TABLE_ERRORS_SUMMARY", errors=_table_errors)
        log("DONE")

    log("MV_REFRESH_PHASE_START")
    pg_execute("REFRESH MATERIALIZED VIEW mv_data_domain_signals;", context={"action": "refresh_mv", "mv": "mv_data_domain_signals"})
    pg_execute("REFRESH MATERIALIZED VIEW mv_unit_signals_by_version;", context={"action": "refresh_mv", "mv": "mv_unit_signals_by_version"})
    log("MV_REFRESH_PHASE_OK", views=["mv_data_domain_signals", "mv_unit_signals_by_version"])

except Exception as e:
    _sync_error = e
    raise

finally:
    _detail = {s["table"]: {"upserts": s.get("upserts", 0), "deletes": s.get("deletes", 0)} for s in all_stats if "table" in s}
    _total_rows = sum(v["upserts"] + v["deletes"] for v in _detail.values())

    _uc_source = {"data_domain_version__derived": "version_scenario_life_time"}
    for tbl in _detail:
        uc_name = _uc_source.get(tbl, tbl)
        try:
            full = config.uc_tbl(uc_name)
            cat, schema, tbl_name = full.split(".")
            row = config.spark.sql(
                f"SELECT last_altered FROM {cat}.information_schema.tables "
                f"WHERE table_schema='{schema}' AND table_name='{tbl_name}'"
            ).collect()
            _detail[tbl]["materialized_at"] = str(row[0]["last_altered"]) if row else None
        except Exception:
            _detail[tbl]["materialized_at"] = None

    _tables_detail = json.dumps(_detail)

    if _sync_error:
        _status = "error"
    elif _table_errors:
        _status = "partial"
    else:
        _status = "success"

    _error_msg = None
    if _sync_error:
        _error_msg = str(_sync_error)
    elif _table_errors:
        _error_msg = json.dumps(_table_errors)

    pg_execute(
        """UPDATE public.master_data_sync_log
           SET ended_at = now(), status = %s, rows_updated = %s,
               error_message = %s, tables_detail = %s
           WHERE id = %s""",
        params=(_status, _total_rows, _error_msg, _tables_detail, _sync_log_id),
    )
