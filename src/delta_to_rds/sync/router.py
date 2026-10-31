from pyspark.sql import functions as F

from src.delta_to_rds import config
from src.delta_to_rds.logger import log
from src.delta_to_rds.uc.utils import (
    table_exists_uc, load_last_version, is_delta_table,
    uc_object_type, latest_delta_version,
)
from src.delta_to_rds.sync.generic import sync_dim_generic
from src.delta_to_rds.sync.plant import sync_plant
from src.delta_to_rds.sync.unit import sync_unit
from src.delta_to_rds.sync.exercise import sync_data_domain_version_from_exercise, sync_exercise
from src.delta_to_rds.sync.signal import sync_signal_life_time


ALL_UC_TABLES = [
    "country_wapp",
    "data_domain_wapp",
    "gbu_wapp",
    "global_finance_wapp",
    "global_technology_wapp",
    "scenario_wapp",
    "signal_wapp",
    "signal_life_time_wapp",
    "unit_status_wapp",
    "version_wapp",
    "plant_wapp",
    "unit_wapp",
    "version_scenario_life_time_wapp",
]

DIM_SPECS = {
    "country_wapp": ("country", {"code": "code", "description": "description"}, None),
    "data_domain_wapp": ("data_domain", {"code": "code", "description": "description"}, None),
    "gbu_wapp": ("gb_information", {"gb_name": "gb_name", "gbu_name": "gbu_name"}, None),
    "global_finance_wapp": (
        "global_finance",
        {
            "smart_package_code": "smart_package_code",
            "smart_package_label": "smart_package_label",
            "finance_entity_code": "smart_entity_code",
            "finance_entity_label": "smart_entity_label",
        },
        {"finance_entity_code": F.lit("null").cast("string")},
    ),
    "global_technology_wapp": ("global_technology_information", {
        "technology": "main_technology",
        "sub_technology": "sub_technology",
        "technology_type": "technology_class",
    }, {
        "technology": F.lit("null").cast("string"),
        "sub_technology": F.lit(None).cast("string"),
        "technology_type": F.lit(None).cast("string"),
    }),
    "version_wapp": ("version", {"code": "code", "description": "description"}, None),
    "scenario_wapp": ("scenario", {"code": "code", "description": "description"}, None),
    "unit_status_wapp": ("unit_status", {"code": "code", "description": "description"}, None),
    "signal_wapp": ("signal", {
        "code": "code",
        "description": "description",
        "type": "type",
        "unit_of_measurement_code": "unit_of_measurement_code",
        "unit_of_measurement_description": "unit_of_measurement_description",
    }, {"type": F.lit(None).cast("string")}),
}

ORDER = [
    "country_wapp",
    "data_domain_wapp",
    "gbu_wapp",
    "global_technology_wapp",
    "global_finance_wapp",
    "version_wapp",
    "scenario_wapp",
    "unit_status_wapp",
    "signal_wapp",
    "plant_wapp",
    "unit_wapp",
    "version_scenario_life_time_wapp",
    "data_domain_version__derived",
    "signal_life_time_wapp",
]


def tables_with_changes():
    changed = []
    for t in ALL_UC_TABLES:
        uc = config.uc_tbl(t)

        if not table_exists_uc(uc):
            log("UC_TABLE_MISSING_SKIP", uc_table=uc)
            continue

        last = load_last_version(t)

        if not is_delta_table(uc):
            log("UC_NOT_DELTA_SNAPSHOT_MODE", uc_table=uc, object_type=uc_object_type(uc))
            changed.append(t)
            continue

        try:
            latest = latest_delta_version(uc)
        except Exception as e:
            log("UC_HISTORY_READ_FAIL", uc_table=uc, err=str(e))
            continue

        if latest > last:
            changed.append(t)

    return changed


def run_one_table(name: str):
    if name in DIM_SPECS:
        pg_name, mapping, fill = DIM_SPECS[name]
        return sync_dim_generic(name, pg_name, mapping, fill_missing=fill)

    if name == "plant_wapp":
        return sync_plant()

    if name == "unit_wapp":
        return sync_unit()

    if name == "version_scenario_life_time_wapp":
        return sync_exercise()

    if name == "data_domain_version__derived":
        return sync_data_domain_version_from_exercise()

    if name == "signal_life_time_wapp":
        return sync_signal_life_time()

    log("UNKNOWN_TABLE", table=name)
    return {"table": name, "processed": False, "mode": "unknown", "skipped_fk": 0, "upserts": 0, "deletes": 0}
