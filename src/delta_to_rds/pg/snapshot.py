from src.delta_to_rds import config
from src.delta_to_rds.pg.helpers import soft_delete_supported, pg_fetchall, soft_delete


def snapshot_df(uc_fullname: str):
    return config.spark.table(uc_fullname)


def snapshot_reconcile_deletes(pg_table: str, snapshot_source_ids: set, context=None):
    context = context or {}
    if not soft_delete_supported(pg_table):
        return 0
    existing = set([r[0] for r in pg_fetchall(f"SELECT source_id FROM {pg_table} WHERE is_deleted=false")])
    missing = list(existing - snapshot_source_ids)
    return soft_delete(pg_table, missing, context=context)
