import json
import boto3


spark = None
dbutils = None

CATALOG = None
SCHEMA_1 = None
SCHEMA_2 = None
UC_SCHEMA_2_TABLES = {"plant_wapp", "unit_wapp"}

STATE_BASE = None

PG_HOST = None
PG_PORT = None
PG_DB = None
PG_USER = None
PG_PWD = None

jdbc_url = None
jdbc_props = None

FULL_RECONCILE_DELETES_ON_SNAPSHOT = True
FALLBACK_TO_SNAPSHOT_ON_CDF_GAP = True

DEBUG_PG_ROW_BY_ROW_ON_ERROR = True
DEBUG_PG_ROW_BY_ROW_LIMIT = 5000
DEBUG_SQL_PREVIEW_CHARS = 300

PRECHECK_UPSERT_CONSTRAINTS = False

ENV_CONFIGS = {
    "dev": {
        "secret_id": "demo-document-bucket",
        "catalog": "asset_api_preprod_3_gold",
        "schema_1": "master_data_webapp",
        "schema_2": "master_data_webapp",
        "state_catalog": "asset_api_webapp_dev",
        "state_suffix": "dev",
    },
    "noprod": {
        "secret_id": "demo-document-bucket",
        "catalog": "asset_api_preprod_3_gold",
        "schema_1": "master_data_webapp",
        "schema_2": "master_data_webapp",
        "state_catalog": "asset_api_webapp_preprod",
        "state_suffix": "nprd",
    },
}


def init(_spark, _dbutils, env="dev"):
    global spark, dbutils
    global CATALOG, SCHEMA_1, SCHEMA_2, STATE_BASE
    global PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PWD
    global jdbc_url, jdbc_props

    spark = _spark
    dbutils = _dbutils

    if env not in ENV_CONFIGS:
        raise ValueError(f"Unknown env '{env}', expected one of {list(ENV_CONFIGS)}")
    ecfg = ENV_CONFIGS[env]

    SERVICE_CREDENTIAL_NAME = "asset_api_nprd_rds_service_credential"
    REGION = "eu-west-1"

    provider = dbutils.credentials.getServiceCredentialsProvider(SERVICE_CREDENTIAL_NAME)
    session = boto3.Session(botocore_session=provider, region_name=REGION)
    sm = session.client("secretsmanager")
    response = sm.get_secret_value(SecretId=ecfg["secret_id"])
    creds = json.loads(response["SecretString"])

    PG_HOST = creds["DB_HOST"]
    PG_PORT = creds["DB_PORT"]
    PG_DB = creds["DB_NAME"]
    PG_USER = creds["DB_USER"]
    PG_PWD = creds["DB_PASSWORD"]

    jdbc_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
    jdbc_props = {"user": PG_USER, "password": PG_PWD, "driver": "org.postgresql.Driver"}

    CATALOG = ecfg["catalog"]
    SCHEMA_1 = ecfg["schema_1"]
    SCHEMA_2 = ecfg["schema_2"]

    STATE_BASE = f"/Volumes/{ecfg['state_catalog']}/default/uc_state/uc_to_rds_state_{ecfg['state_suffix']}"


def uc_tbl(t: str) -> str:
    schema = SCHEMA_2 if t in UC_SCHEMA_2_TABLES else SCHEMA_1
    return f"{CATALOG}.{schema}.{t}"


def state_path(t: str) -> str:
    return f"{STATE_BASE}/{t}.json"
