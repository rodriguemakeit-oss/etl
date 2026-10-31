# Lakebridge ETL

Trois points d’entrée Databricks orchestrent les traitements :

- `main_sync_delta_to_rds.py` : synchroniser les dimensions Delta vers PostgreSQL.
- `main_sync_kpi_data_to_rds.py` : valider et transmettre des fichiers KPI.
- `main_sync_pg_kpi_data_to_delta_table.py` : synchroniser les KPI PostgreSQL vers Delta.

Le code réutilisable se trouve dans `src/`. Les notebooks sont conservés sans sorties de cellules.

Configurez `databricks.yml`, les catalogues et les scopes de secrets dans les modules `config.py`. Les accès PostgreSQL proviennent d’un gestionnaire de secrets et les accès API utilisent une configuration OAuth.

Le runtime attendu est Databricks avec Spark, `dbutils`, les drivers PostgreSQL et `boto3`. Aucun accès réseau ni déploiement réel n’a été testé localement.
