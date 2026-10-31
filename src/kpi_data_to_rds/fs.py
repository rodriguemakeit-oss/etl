from src.kpi_data_to_rds import config


def safe_ls(path):
    try:
        return config.dbutils.fs.ls(path)
    except Exception:
        return []


def ensure_dir(path):
    try:
        config.dbutils.fs.mkdirs(path)
    except Exception:
        pass


def move_to(src_path, dest_dir):
    ensure_dir(dest_dir)
    name = src_path.split("/")[-1]
    dest = dest_dir.rstrip("/") + "/" + name
    config.dbutils.fs.mv(src_path, dest)
    return dest


def write_text(path, content):
    ensure_dir(path.rsplit("/", 1)[0] + "/")
    config.dbutils.fs.put(path, content, overwrite=True)
