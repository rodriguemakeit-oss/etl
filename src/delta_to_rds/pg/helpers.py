import time
import psycopg2
from psycopg2.extras import execute_batch, execute_values
from pyspark.sql import functions as F

from src.delta_to_rds import config
from src.delta_to_rds.logger import log


def _merge_ctx(base: dict | None, extra: dict | None):
    d = {}
    if base:
        d.update(base)
    if extra:
        d.update(extra)
    return d


def pg_conn():
    return psycopg2.connect(
        host=config.PG_HOST, port=config.PG_PORT,
        database=config.PG_DB, user=config.PG_USER, password=config.PG_PWD
    )


def _pg_exc_details(e: Exception):
    diag = getattr(e, "diag", None)
    return {
        "err_type": type(e).__name__,
        "err_msg": str(e),
        "pgcode": getattr(e, "pgcode", None),
        "pgerror": getattr(e, "pgerror", None),
        "diag_sqlstate": getattr(diag, "sqlstate", None) if diag else None,
        "diag_message_primary": getattr(diag, "message_primary", None) if diag else None,
        "diag_constraint_name": getattr(diag, "constraint_name", None) if diag else None,
        "diag_table_name": getattr(diag, "table_name", None) if diag else None,
        "diag_column_name": getattr(diag, "column_name", None) if diag else None,
    }


def pg_table_columns(pg_table: str):
    conn = pg_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name = %s
        """, (pg_table,))
        return set([r[0] for r in cur.fetchall()])
    finally:
        cur.close()
        conn.close()


NOT_NULL_STRING_DEFAULTS = {
    "global_technology_information": {"technology": "null"},
    "global_finance": {
        "smart_package_code": "null",
        "finance_entity_code": "null",
    },
}


def apply_not_null_default(pg_table: str, pg_col: str, col_expr):
    default = NOT_NULL_STRING_DEFAULTS.get(pg_table, {}).get(pg_col)
    if default is None:
        return col_expr
    c = col_expr.cast("string")
    return F.when(c.isNull() | (F.trim(c) == ""), F.lit(default)).otherwise(c)


def build_mapped_exprs(base_df, pg_table: str, mapping: dict, fill_missing: dict | None):
    cols_uc = set(base_df.columns)
    exprs = []

    for pg_c, uc_c in mapping.items():
        if uc_c in cols_uc:
            c = F.col(uc_c).cast("string")
            c = apply_not_null_default(pg_table, pg_c, c)
            exprs.append(c.alias(pg_c))
        else:
            if fill_missing and pg_c in fill_missing:
                c = fill_missing[pg_c]
                c = apply_not_null_default(pg_table, pg_c, c)
                exprs.append(c.alias(pg_c))
            else:
                default = NOT_NULL_STRING_DEFAULTS.get(pg_table, {}).get(pg_c)
                if default is not None:
                    exprs.append(F.lit(default).cast("string").alias(pg_c))
                else:
                    exprs.append(F.lit(None).cast("string").alias(pg_c))

    return exprs


def pg_fetchall(sql: str, params=None):
    conn = pg_conn()
    cur = conn.cursor()
    try:
        cur.execute(sql, params or ())
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()


def pg_fetchone(sql: str, params=None):
    conn = pg_conn()
    cur = conn.cursor()
    try:
        cur.execute(sql, params or ())
        row = cur.fetchone()
        conn.commit()
        return row
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def pg_execute_many(sql: str, rows: list, page_size=1000, context=None):
    if not rows:
        return 0

    ctx = context or {}
    conn = pg_conn()
    cur = conn.cursor()
    try:
        log("PG_EXECUTE_MANY_START", rows=len(rows), page_size=page_size, **ctx)
        execute_batch(cur, sql, rows, page_size=page_size)
        conn.commit()
        log("PG_EXECUTE_MANY_OK", rows=len(rows), **ctx)
        return len(rows)
    except Exception as e:
        conn.rollback()
        diag = getattr(e, "diag", None)
        log(
            "PG_EXECUTE_MANY_ERROR",
            err_type=type(e).__name__,
            err=str(e),
            diag_message_primary=getattr(diag, "message_primary", None) if diag else None,
            diag_constraint_name=getattr(diag, "constraint_name", None) if diag else None,
            diag_column_name=getattr(diag, "column_name", None) if diag else None,
            diag_table_name=getattr(diag, "table_name", None) if diag else None,
            **ctx
        )
        raise
    finally:
        cur.close()
        conn.close()


def pg_execute_values(sql: str, rows: list, page_size=1000, context=None):
    if not rows:
        return 0

    ctx = context or {}
    conn = pg_conn()
    cur = conn.cursor()
    try:
        log("PG_EXECUTE_VALUES_START", rows=len(rows), page_size=page_size, **ctx)
        results = execute_values(cur, sql, rows, page_size=page_size, fetch=True)
        affected = len(results)
        conn.commit()
        log("PG_EXECUTE_VALUES_OK", rows=len(rows), affected=affected, **ctx)
        return affected
    except Exception as e:
        conn.rollback()
        diag = getattr(e, "diag", None)
        log(
            "PG_EXECUTE_VALUES_ERROR",
            err_type=type(e).__name__,
            err=str(e),
            diag_message_primary=getattr(diag, "message_primary", None) if diag else None,
            diag_constraint_name=getattr(diag, "constraint_name", None) if diag else None,
            diag_column_name=getattr(diag, "column_name", None) if diag else None,
            diag_table_name=getattr(diag, "table_name", None) if diag else None,
            **ctx
        )
        raise
    finally:
        cur.close()
        conn.close()


def pg_execute(sql: str, params=None, context: dict = None):
    context = context or {}
    conn = pg_conn()
    cur = conn.cursor()
    t0 = time.time()

    try:
        log("PG_EXECUTE_START", **context)
        cur.execute(sql, params or ())
        conn.commit()
        log("PG_EXECUTE_OK", elapsed_s=round(time.time() - t0, 3), **context)
    except Exception as e:
        conn.rollback()
        det = _pg_exc_details(e)
        log("PG_EXECUTE_ERROR",
            elapsed_s=round(time.time() - t0, 3),
            sql_preview=sql.strip().replace("\n", " ")[:config.DEBUG_SQL_PREVIEW_CHARS],
            **det, **context
        )
        raise
    finally:
        cur.close()
        conn.close()


def pg_has_unique_on(pg_table: str, col: str) -> bool:
    sql = """
    SELECT EXISTS (
      SELECT 1
      FROM pg_index i
      JOIN pg_class t ON t.oid = i.indrelid
      JOIN pg_attribute a ON a.attrelid = t.oid
      WHERE t.relname = %s
        AND i.indisunique
        AND a.attnum = ANY(i.indkey)
        AND a.attname = %s
    );
    """
    return bool(pg_fetchall(sql, (pg_table, col))[0][0])


def soft_delete_supported(pg_table: str) -> bool:
    cols = pg_table_columns(pg_table)
    return ("is_deleted" in cols) and ("deleted_at" in cols)


def soft_delete(pg_table: str, source_ids: list, context=None):
    if not source_ids:
        return 0

    ctx = _merge_ctx({"pg_table": pg_table}, context)

    if not soft_delete_supported(pg_table):
        log("SOFT_DELETE_SKIPPED_NO_COLS", count=len(source_ids), **ctx)
        return 0

    rows = [(sid,) for sid in source_ids if sid is not None]
    sql = f"UPDATE {pg_table} SET is_deleted=true, deleted_at=now() WHERE source_id=%s"

    log("SOFT_DELETE_START", count=len(rows), **ctx)
    pg_execute_many(sql, rows, context={**ctx, "action": "soft_delete"})
    log("SOFT_DELETE_OK", count=len(rows), **ctx)
    return len(rows)


def upsert_rows(
    pg_table: str,
    cols: list,
    rows: list,
    conflict_col="source_id",
    reactivate=True,
    context=None,
    exclude_update_cols: list | None = None,
):
    if not rows:
        return 0

    conflict_idx = cols.index(conflict_col)
    seen = {}
    for row in rows:
        seen[row[conflict_idx]] = row
    rows = list(seen.values())

    ctx = _merge_ctx({"pg_table": pg_table}, context)
    exclude_update_cols = set(exclude_update_cols or [])

    cols_sql = ", ".join(cols)

    upd_cols = [c for c in cols if c not in (["id", conflict_col] + list(exclude_update_cols))]
    set_clause = ", ".join([f"{c}=EXCLUDED.{c}" for c in upd_cols])

    if reactivate and soft_delete_supported(pg_table):
        set_clause = (set_clause + ", is_deleted=false, deleted_at=NULL") if set_clause else "is_deleted=false, deleted_at=NULL"

    if not set_clause:
        sql = f"""
            INSERT INTO {pg_table} ({cols_sql})
            VALUES %s
            ON CONFLICT ({conflict_col}) DO NOTHING
            RETURNING 1
        """
    else:
        where_parts = [f"{pg_table}.{c} IS DISTINCT FROM EXCLUDED.{c}" for c in upd_cols]
        if reactivate and soft_delete_supported(pg_table):
            where_parts.append(f"{pg_table}.is_deleted IS DISTINCT FROM false")
        where_clause = f" WHERE {' OR '.join(where_parts)}" if where_parts else ""

        sql = f"""
            INSERT INTO {pg_table} ({cols_sql})
            VALUES %s
            ON CONFLICT ({conflict_col}) DO UPDATE SET {set_clause}{where_clause}
            RETURNING 1
        """

    log("UPSERT_START", conflict_col=conflict_col, cols=len(cols), rows=len(rows), excluded_update=list(exclude_update_cols), **ctx)
    affected = pg_execute_values(sql, rows, context={**ctx, "action": "upsert", "conflict_col": conflict_col})
    log("UPSERT_OK", sent=len(rows), affected=affected, **ctx)
    return affected


def upsert_rows_on_cols(pg_table: str, cols: list, rows: list, conflict_cols: list, reactivate=False, context=None, exclude_update_cols: list | None = None):
    if not rows:
        return 0

    conflict_idxs = [cols.index(c) for c in conflict_cols]
    seen = {}
    for row in rows:
        key = tuple(row[i] for i in conflict_idxs)
        seen[key] = row
    rows = list(seen.values())

    ctx = _merge_ctx({"pg_table": pg_table}, context)
    exclude_update_cols = set(exclude_update_cols or [])

    cols_sql = ", ".join(cols)
    conflict_sql = ", ".join(conflict_cols)

    upd_cols = [c for c in cols if c not in ["id"] + conflict_cols + list(exclude_update_cols)]
    set_clause = ", ".join([f"{c}=EXCLUDED.{c}" for c in upd_cols])

    if not set_clause:
        sql = f"""
            INSERT INTO {pg_table} ({cols_sql})
            VALUES %s
            ON CONFLICT ({conflict_sql}) DO NOTHING
            RETURNING 1
        """
    else:
        where_parts = [f"{pg_table}.{c} IS DISTINCT FROM EXCLUDED.{c}" for c in upd_cols]
        where_clause = f" WHERE {' OR '.join(where_parts)}" if where_parts else ""

        sql = f"""
            INSERT INTO {pg_table} ({cols_sql})
            VALUES %s
            ON CONFLICT ({conflict_sql}) DO UPDATE SET {set_clause}{where_clause}
            RETURNING 1
        """

    log("UPSERT_COMPOSITE_START", conflict_cols=conflict_cols, cols=len(cols), rows=len(rows), **ctx)
    affected = pg_execute_values(sql, rows, context={**ctx, "action": "upsert_composite", "conflict_cols": conflict_cols})
    log("UPSERT_COMPOSITE_OK", sent=len(rows), affected=affected, **ctx)
    return affected
