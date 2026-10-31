from datetime import datetime

S3_INCOMING = "s3://demo-document-bucket/incoming/"
S3_ACCEPTED = "s3://demo-document-bucket/accepted/"
S3_REJECTED = "s3://demo-document-bucket/rejected/"
S3_LOGS_REJ = "s3://demo-document-bucket/logs/rejected/"
S3_LOGS_API = "s3://demo-document-bucket/logs/api_upload/"

CHECKPOINT_TABLE = "asset_api_webapp_dev.default.kpi_state"

VOLUME_EXCEL_DIR_DBFS = "dbfs:/Volumes/asset_api_webapp_dev/default/kpi_parquet/excel_exports/"

N_MAX = 1000
SAMPLE_N = 50

BASE_URL = "https://service.example.invalid"
UPLOAD_URL = f"{BASE_URL}/kpi-data/upload-excel"
OKTA_TOKEN_URL = "https://service.example.invalid"

spark = None
dbutils = None
run_ts = None
CLIENT_ID = None
CLIENT_SECRET = None


def init(_spark, _dbutils):
    global spark, dbutils, run_ts, CLIENT_ID, CLIENT_SECRET
    spark = _spark
    dbutils = _dbutils
    run_ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    CLIENT_ID = dbutils.secrets.get("okta-secret", "client_id_dev")
    CLIENT_SECRET = dbutils.secrets.get("okta-secret", "client_secret_dev")
