import os
import requests
from requests.auth import HTTPBasicAuth
from src.kpi_data_to_rds import config


def get_access_token(scope="api.write"):
    r = requests.post(
        config.OKTA_TOKEN_URL,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={"grant_type": "client_credentials", "scope": scope},
        auth=HTTPBasicAuth(config.CLIENT_ID, config.CLIENT_SECRET),
        timeout=30,
    )
    if r.status_code != 200:
        raise RuntimeError(f"Okta token error {r.status_code}: {r.text}")
    return r.json()["access_token"]


def upload_excel(excel_file_path, data_domain_id, gid):
    token = get_access_token("api.write")

    headers = {
        "Authorization": f"Bearer {token}",
        "gid": gid,
        "source": "s3",
    }

    with open(excel_file_path, "rb") as f:
        r = requests.post(
            config.UPLOAD_URL,
            params={"data_domain_id": data_domain_id},
            headers=headers,
            files={
                "file": (
                    os.path.basename(excel_file_path),
                    f,
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            },
            timeout=300,
        )

    if r.status_code >= 400:
        raise RuntimeError(f"Upload error {r.status_code}: {r.text}")

    return r.json()
