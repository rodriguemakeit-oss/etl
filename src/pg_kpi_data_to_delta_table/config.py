import boto3
import json

SERVICE_CREDENTIAL_NAME = "asset_api_nprd_rds_service_credential"
REGION = "eu-west-1"

CATALOG = "asset_api_webapp_dev"
SCHEMA = "default"

spark = None
dbutils = None

DB_URL = None
DB_PROPS = None
WATERMARK_TABLE = None

TABLES = [
    {
        "pg_table": "public.kpi_log",
        "delta_table": None,
        "pk_col": "id",
        "created_col": "changed_at",
        "updated_col": "changed_at",
        "fk_mappings": [],
    },
    {
        "pg_table": "public.kpi_data",
        "delta_table": None,
        "pk_col": "id",
        "created_col": "created_date",
        "updated_col": "updated_date",
        "fk_mappings": [
            ("signal_id", "public.signal", "id", "source_id", "signal_source_id"),
            ("unit_id", "public.unit", "id", "source_id", "unit_source_id"),
            ("version_id", "public.version", "id", "source_id", "version_source_id"),
        ],
    },
    {
        "pg_table": "public.kpi_comment",
        "delta_table": None,
        "pk_col": "id",
        "created_col": "created_date",
        "updated_col": "updated_date",
        "fk_mappings": [
            ("signal_id", "public.signal", "id", "source_id", "signal_source_id"),
            ("unit_id", "public.unit", "id", "source_id", "unit_source_id"),
        ],
    },
]


def init(_spark, _dbutils):
    global spark, dbutils, DB_URL, DB_PROPS, WATERMARK_TABLE

    spark = _spark
    dbutils = _dbutils

    provider = dbutils.credentials.getServiceCredentialsProvider(SERVICE_CREDENTIAL_NAME)
    session = boto3.Session(botocore_session=provider, region_name=REGION)
    sm = session.client("secretsmanager")

    response = sm.get_secret_value(SecretId="demo-document-bucket")
    creds = json.loads(response["SecretString"])

    DB_URL = f"jdbc:postgresql://{creds['DB_HOST']}:{creds['DB_PORT']}/{creds['DB_NAME']}"
    DB_PROPS = {
        "user": creds["DB_USER"],
        "password": creds["DB_PASSWORD"],
        "driver": "org.postgresql.Driver",
        "fetchsize": "10000",
    }

    WATERMARK_TABLE = f"{CATALOG}.{SCHEMA}._rds_mirror_watermarks_v3_fk_source_ids"

    for t in TABLES:
        table_name = t["pg_table"].replace("public.", "")
        t["delta_table"] = f"{CATALOG}.{SCHEMA}.{table_name}_delta"
