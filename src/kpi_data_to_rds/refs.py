from pyspark.sql import functions as F
from src.kpi_data_to_rds import config


def load_refs():
    s = config.spark

    unit = s.table("asset_api_webapp_dev.default.unit").select(
        F.col("code").alias("asset_code"),
        "plant_id",
        F.col("description").alias("asset_name"),
        "global_technology_id",
    )

    plant = s.table("asset_api_webapp_dev.default.plant").select(
        F.col("id").alias("plant_id"),
        "country_id",
    )

    country = s.table("asset_api_webapp_dev.default.country").select(
        F.col("id").alias("country_id"),
        F.col("code").alias("country_code"),
        F.col("description").alias("country_desc"),
    )

    data_domain = s.table("asset_api_webapp_dev.default.data_domain").select(
        F.col("code").alias("data_domain_code"),
    )

    version = s.table("asset_api_webapp_dev.default.version").select(
        F.col("id").alias("version_id"),
        F.col("code").alias("version_code"),
    )

    scenario = s.table("asset_api_webapp_dev.default.scenario").select(
        F.col("id").alias("scenario_id"),
        F.col("code").alias("scenario_code"),
    )

    exercise = s.table("asset_api_webapp_dev.default.exercise").select(
        "scenario_id", "version_id",
    )

    gti = s.table("asset_api_webapp_dev.default.global_technology").select(
        F.col("id").alias("global_technology_id"),
        F.col("main_technology").alias("Technology"),
    )

    return {
        "unit": unit,
        "plant": plant,
        "country": country,
        "data_domain": data_domain,
        "version": version,
        "scenario": scenario,
        "exercise": exercise,
        "gti": gti,
    }
