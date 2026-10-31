from src.delta_to_rds import config
from src.delta_to_rds.logger import log
from src.delta_to_rds.uc.utils import is_delta_table


def read_cdf_batch(uc_table: str, start_version: int, end_version: int):
    if start_version > end_version:
        return None

    if not is_delta_table(uc_table):
        return None

    try:
        return (config.spark.read.format("delta")
                .option("readChangeFeed", "true")
                .option("startingVersion", start_version)
                .option("endingVersion", end_version)
                .table(uc_table))
    except Exception as e:
        msg = str(e)
        if "DELTA_MISSING_CHANGE_DATA" in msg or "Missing change data" in msg:
            if config.FALLBACK_TO_SNAPSHOT_ON_CDF_GAP:
                log("CDF_GAP_FALLBACK_SNAPSHOT", uc_table=uc_table, start=start_version, end=end_version, err=msg)
                return None
        raise
