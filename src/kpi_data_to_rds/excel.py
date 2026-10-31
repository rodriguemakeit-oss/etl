import os
import uuid
import pandas as pd
from pyspark.sql import functions as F
from src.kpi_data_to_rds import config


def build_kpi2(kpi_file_df, refs):
    kpi = kpi_file_df.withColumn(
        "Period",
        F.when(
            F.col("PERIOD").rlike(r"^\d{2}/\d{4}$"),
            F.concat_ws(".", F.substring("PERIOD", 4, 4), F.substring("PERIOD", 1, 2))
        ).otherwise(F.col("PERIOD"))
    )

    kpi = (kpi
        .withColumnRenamed("DOMAIN", "Data Domain")
        .withColumnRenamed("UNIT", "Asset Code")
        .withColumnRenamed("VERSION", "Version")
        .withColumnRenamed("SCENARIO", "Scenario")
    )

    unit_dim = refs["unit"].select(
        F.col("asset_code").alias("Asset Code"),
        F.col("asset_name").alias("Asset Name"),
        "plant_id",
        "global_technology_id",
    )

    country_dim = refs["country"].select(
        "country_id",
        F.col("country_desc").alias("Country"),
    )

    kpi2 = (kpi
        .join(unit_dim, on="Asset Code", how="left")
        .join(refs["plant"], on="plant_id", how="left")
        .join(country_dim, on="country_id", how="left")
        .withColumn("Geozone", F.col("Country"))
        .join(refs["gti"], on="global_technology_id", how="left")
        .select(
            "USERNAME", "GID", "Data Domain", "Geozone", "Country", "Technology",
            "Asset Code", "Asset Name", "Version", "Scenario", "Period",
            "ITEM", "QUANTITY", "COMMENT", "isDeleted",
        )
    )
    return kpi2


def export_to_excel_local(kpi2_df):
    local_dir = f"/tmp/asset_api_excel/{config.run_ts}"
    os.makedirs(local_dir, exist_ok=True)
    local_xlsx = f"{local_dir}/{uuid.uuid4().hex}.xlsx"

    pdf = kpi2_df.toPandas()
    with pd.ExcelWriter(local_xlsx, engine="openpyxl") as writer:
        pdf.to_excel(writer, index=False, sheet_name="kpi2")

    return local_xlsx
