# Databricks notebook source

from databricks.sdk.runtime import spark, dbutils

from src.kpi_data_to_rds import config
from src.kpi_data_to_rds.refs import load_refs
from src.kpi_data_to_rds.pipeline import discover_new_files, validate_and_move, upload_accepted

# COMMAND ----------

config.init(spark, dbutils)
refs = load_refs()

# COMMAND ----------

new_paths = discover_new_files()
print(f"Nouveaux fichiers trouvés: {len(new_paths)}")
for p in new_paths[:20]:
    print(" -", p)

if len(new_paths) == 0:
    print("Rien à faire.")
    dbutils.notebook.exit("NO_NEW_FILES")

# COMMAND ----------

accepted = validate_and_move(new_paths, refs)

if len(accepted) == 0:
    print("Aucun fichier accepté -> fin du job.")
    dbutils.notebook.exit("NO_ACCEPTED_FILES")

# COMMAND ----------

upload_accepted(accepted, refs)
