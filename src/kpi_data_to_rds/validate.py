from pyspark.sql import functions as F
from src.kpi_data_to_rds import config


def normalize_kpi_stg(kpi):
    return (kpi
        .withColumn(
            "Period",
            F.when(
                F.col("PERIOD").rlike(r"^\d{2}/\d{4}$"),
                F.concat_ws(".", F.substring("PERIOD", 4, 4), F.substring("PERIOD", 1, 2))
            ).otherwise(F.col("PERIOD"))
        )
        .withColumnRenamed("DOMAIN", "data_domain_code")
        .withColumnRenamed("UNIT", "asset_code")
        .withColumnRenamed("VERSION", "version_code")
        .withColumnRenamed("SCENARIO", "scenario_code")
        .withColumnRenamed("ITEM", "item_code")
    )


def validate_kpi_parquet_simple(kpi_stg, refs, sample_n=None):
    if sample_n is None:
        sample_n = config.SAMPLE_N

    unit_df = refs["unit"].select("asset_code", "plant_id")
    plant_df = refs["plant"]
    country_df = refs["country"]
    data_domain_df = refs["data_domain"]
    version_df = refs["version"]
    scenario_df = refs["scenario"]
    exercise_df = refs["exercise"]

    reasons = []

    missing_unit = kpi_stg.join(unit_df.select("asset_code").distinct(), on="asset_code", how="left_anti")
    if missing_unit.take(1):
        reasons.append({
            "rule": "UNKNOWN_ASSET_CODE",
            "message": "Le fichier référence au moins un Asset Code absent de la table unit.",
            "examples": missing_unit.select("asset_code").distinct().limit(sample_n).toPandas().to_dict("records"),
        })

    kpi_country = (kpi_stg
        .join(unit_df, on="asset_code", how="left")
        .join(plant_df, on="plant_id", how="left")
        .join(country_df.select("country_id", "country_code"), on="country_id", how="left")
    )

    missing_country = kpi_country.filter(F.col("country_code").isNull())
    if missing_country.take(1):
        reasons.append({
            "rule": "UNKNOWN_COUNTRY",
            "message": "Le fichier référence un country inexistant (via asset_code -> unit -> plant -> country).",
            "examples": (missing_country
                         .select("asset_code", "plant_id", "country_id")
                         .distinct()
                         .limit(sample_n)
                         .toPandas()
                         .to_dict("records")),
        })

    missing_dd = kpi_stg.join(
        data_domain_df.select("data_domain_code").distinct(),
        on="data_domain_code", how="left_anti",
    )
    if missing_dd.take(1):
        reasons.append({
            "rule": "UNKNOWN_DATA_DOMAIN",
            "message": "Le fichier référence un Data Domain absent de la table data_domain.",
            "examples": missing_dd.select("data_domain_code").distinct().limit(sample_n).toPandas().to_dict("records"),
        })

    dup_keys = ["data_domain_code", "country_code", "version_code", "scenario_code", "Period", "item_code", "asset_code"]
    dups = (kpi_country
        .groupBy(*dup_keys)
        .count()
        .filter(F.col("count") > 1)
    )
    if dups.take(1):
        reasons.append({
            "rule": "DUPLICATE_KEYS",
            "message": f"Doublons détectés: >1 enregistrement pour la même clé {dup_keys} (même si QUANTITY diffère).",
            "examples": dups.limit(sample_n).toPandas().to_dict("records"),
        })

    kpi_vs = (kpi_stg
        .join(version_df, on="version_code", how="left")
        .join(scenario_df, on="scenario_code", how="left")
    )

    unknown_version = kpi_vs.filter(F.col("version_id").isNull()).select("version_code").distinct()
    if unknown_version.take(1):
        reasons.append({
            "rule": "UNKNOWN_VERSION_CODE",
            "message": "Le fichier référence au moins un Version code absent de la table version.",
            "examples": unknown_version.limit(sample_n).toPandas().to_dict("records"),
        })

    unknown_scenario = kpi_vs.filter(F.col("scenario_id").isNull()).select("scenario_code").distinct()
    if unknown_scenario.take(1):
        reasons.append({
            "rule": "UNKNOWN_SCENARIO_CODE",
            "message": "Le fichier référence au moins un Scenario code absent de la table scenario.",
            "examples": unknown_scenario.limit(sample_n).toPandas().to_dict("records"),
        })

    pairs = (kpi_vs
        .select("version_id", "scenario_id")
        .distinct()
        .filter(F.col("version_id").isNotNull() & F.col("scenario_id").isNotNull())
    )
    missing_pairs = pairs.join(exercise_df.distinct(), on=["version_id", "scenario_id"], how="left_anti")
    if missing_pairs.take(1):
        missing_pairs_codes = (missing_pairs
            .join(version_df, on="version_id", how="left")
            .join(scenario_df, on="scenario_id", how="left")
            .select("version_code", "scenario_code")
            .distinct()
        )
        reasons.append({
            "rule": "UNKNOWN_VERSION_SCENARIO_IN_EXERCISE",
            "message": "Au moins un couple (Version, Scenario) du fichier n'existe pas dans exercise.",
            "examples": missing_pairs_codes.limit(sample_n).toPandas().to_dict("records"),
        })

    return {"is_valid": len(reasons) == 0, "reasons": reasons}
