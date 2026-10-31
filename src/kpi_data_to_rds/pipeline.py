import json
from pyspark.sql import functions as F
from src.kpi_data_to_rds import config
from src.kpi_data_to_rds.fs import safe_ls, ensure_dir, move_to, write_text
from src.kpi_data_to_rds.validate import normalize_kpi_stg, validate_kpi_parquet_simple
from src.kpi_data_to_rds.excel import build_kpi2, export_to_excel_local
from src.kpi_data_to_rds.api import upload_excel


def discover_new_files():
    ensure_dir(config.S3_ACCEPTED)
    ensure_dir(config.S3_REJECTED)
    ensure_dir(config.S3_LOGS_REJ)
    ensure_dir(config.S3_LOGS_API)

    try:
        processed_df = config.spark.table(config.CHECKPOINT_TABLE)
        processed = set(r["path"] for r in processed_df.select("path").collect())
    except Exception:
        processed = set()

    files = [f for f in safe_ls(config.S3_INCOMING) if f.path.lower().endswith(".parquet")]
    new_files = [f for f in files if f.path not in processed]
    new_files = sorted(new_files, key=lambda x: x.modificationTime)[:config.N_MAX]
    return [f.path for f in new_files]


def validate_and_move(new_paths, refs):
    accepted = []
    rejected = []
    checkpoint_rows = []

    for src_path in new_paths:
        print(f"\n=== Validation fichier: {src_path} ===")
        try:
            kpi_file = config.spark.read.parquet(src_path)
            kpi_stg_file = normalize_kpi_stg(kpi_file)

            res = validate_kpi_parquet_simple(kpi_stg_file, refs)

            if res["is_valid"]:
                dst = move_to(src_path, config.S3_ACCEPTED)
                accepted.append(dst)
                checkpoint_rows.append((src_path, "accepted", config.run_ts, None))
                print(f"ACCEPTED -> {dst}")
            else:
                dst = move_to(src_path, config.S3_REJECTED)
                rejected.append(dst)

                log_payload = {
                    "run_ts": config.run_ts,
                    "source_file": src_path,
                    "rejected_file": dst,
                    "validation": res,
                    "notes": {"sample_n": config.SAMPLE_N},
                }
                log_name = src_path.split("/")[-1].replace(".parquet", "")
                log_path = f"{config.S3_LOGS_REJ.rstrip('/')}/rejected_{config.run_ts}_{log_name}.json"
                write_text(log_path, json.dumps(log_payload, indent=2, ensure_ascii=False))

                checkpoint_rows.append((src_path, "rejected", config.run_ts, log_path))
                print(f"REJECTED -> {dst}")
                print(f"   Log -> {log_path}")

        except Exception as e:
            dst = move_to(src_path, config.S3_REJECTED)
            rejected.append(dst)

            log_payload = {
                "run_ts": config.run_ts,
                "source_file": src_path,
                "rejected_file": dst,
                "error": str(e),
            }
            log_name = src_path.split("/")[-1].replace(".parquet", "")
            log_path = f"{config.S3_LOGS_REJ.rstrip('/')}/rejected_{config.run_ts}_{log_name}_TECH.json"
            write_text(log_path, json.dumps(log_payload, indent=2, ensure_ascii=False))

            checkpoint_rows.append((src_path, "rejected_tech", config.run_ts, log_path))
            print(f"REJECTED (TECH) -> {dst}")
            print(f"   Log -> {log_path}")

    cp_df = (config.spark
        .createDataFrame(checkpoint_rows, ["path", "status", "run_ts", "log_path"])
        .withColumn("processed_at", F.current_timestamp())
    )
    cp_df.write.format("delta").mode("append").saveAsTable(config.CHECKPOINT_TABLE)

    print(f"\nAccepted: {len(accepted)}")
    print(f"Rejected: {len(rejected)}")
    return accepted


def upload_accepted(accepted_paths, refs):
    if not accepted_paths:
        print("Aucun fichier accepté -> fin du job.")
        return

    run_volume_dir = f"{config.VOLUME_EXCEL_DIR_DBFS}{config.run_ts}/"
    config.dbutils.fs.mkdirs(run_volume_dir)

    success = []
    failed = []

    for accepted_path in accepted_paths:
        file_name = accepted_path.split("/")[-1]
        base_name = file_name.replace(".parquet", "")
        print(f"\n=== Export+Upload: {accepted_path} ===")

        try:
            kpi_file = config.spark.read.parquet(accepted_path)
            kpi2_file = build_kpi2(kpi_file, refs)

            row = kpi2_file.select("USERNAME", "Data Domain", "GID").first()
            if row is None:
                raise ValueError(f"kpi2 is empty for file {accepted_path}")

            gid = str(row["GID"]).lower()
            data_domain_id = str(row["Data Domain"])

            excel_local = export_to_excel_local(kpi2_file.drop("GID"))
            resp = upload_excel(excel_local, data_domain_id=data_domain_id, gid=gid)

            print(f"Uploaded {file_name} -> {resp}")
            success.append({
                "file": accepted_path,
                "gid": gid,
                "data_domain_id": data_domain_id,
                "response": resp,
            })

        except Exception as e:
            err_payload = {
                "run_ts": config.run_ts,
                "accepted_file": accepted_path,
                "file_name": file_name,
                "error": str(e),
            }
            log_path = f"{config.S3_LOGS_API.rstrip('/')}/upload_failed_{config.run_ts}_{base_name}.json"
            write_text(log_path, json.dumps(err_payload, indent=2, ensure_ascii=False))

            print(f"Upload failed for {file_name} -> {e}")
            print(f"   Log -> {log_path}")
            failed.append({"file": accepted_path, "error": str(e), "log_path": log_path})

    print(f"\nUpload OK: {len(success)}")
    print(f"Upload FAILED: {len(failed)}")
