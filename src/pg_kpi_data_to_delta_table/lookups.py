from src.pg_kpi_data_to_delta_table import config


def read_lookup(table, pk, source_col):
    q = f"(SELECT {pk} AS lk_id, {source_col} AS lk_source_id FROM {table}) AS q"
    return config.spark.read.jdbc(config.DB_URL, q, properties=config.DB_PROPS)


def read_user_gid_lookup():
    q = """
    (
      SELECT
        id        AS lk_user_id,
        gid       AS lk_user_gid,
        firstname AS lk_user_firstname,
        lastname  AS lk_user_lastname
      FROM public."user"
    ) AS q
    """
    return config.spark.read.jdbc(config.DB_URL, q, properties=config.DB_PROPS)


def read_exercise_enrich_lookup():
    q = """
    (
      SELECT
        e.id AS lk_exercise_id,
        e.source_id AS lk_exercise_source_id,
        sc.source_id AS lk_scenario_id,
        dd.source_id AS lk_data_domain_source_id
      FROM public.exercise e
      LEFT JOIN public.scenario sc
        ON e.scenario_id = sc.id
      LEFT JOIN public.data_domain_version ddv
        ON e.data_domain_version_id = ddv.id
      LEFT JOIN public.data_domain dd
        ON ddv.data_domain_id = dd.id
    ) AS q
    """
    return config.spark.read.jdbc(config.DB_URL, q, properties=config.DB_PROPS)


def read_kpi_data_enrich_lookup():
    q = """
    (
      SELECT
        kd.id AS lk_kpi_data_id,
        kd.granularity  AS lk_granularity,
        kd.period_year  AS lk_period_year,
        kd.period_month AS lk_period_month,
        e.source_id  AS lk_exercise_source_id,
        sc.source_id AS lk_exercise_scenario_id,
        dd.source_id AS lk_data_domain_source_id,
        sig.source_id AS lk_signal_source_id,
        u.source_id   AS lk_unit_source_id,
        v.source_id   AS lk_version_source_id
      FROM public.kpi_data kd
      LEFT JOIN public.exercise e ON kd.exercise_id = e.id
      LEFT JOIN public.scenario sc ON e.scenario_id = sc.id
      LEFT JOIN public.data_domain_version ddv ON e.data_domain_version_id = ddv.id
      LEFT JOIN public.data_domain dd ON ddv.data_domain_id = dd.id
      LEFT JOIN public.signal sig ON kd.signal_id = sig.id
      LEFT JOIN public.unit u ON kd.unit_id = u.id
      LEFT JOIN public.version v ON kd.version_id = v.id
    ) AS q
    """
    return config.spark.read.jdbc(config.DB_URL, q, properties=config.DB_PROPS)


def read_kpi_comment_latest_lookup():
    q = """
    (
      SELECT
        kc.kpi_data_id AS lk_kpi_data_id,
        kc.comment     AS lk_comment
      FROM public.kpi_comment kc
      JOIN (
        SELECT
          kpi_data_id,
          MAX(COALESCE(updated_date, created_date)) AS max_dt
        FROM public.kpi_comment
        GROUP BY kpi_data_id
      ) last
        ON last.kpi_data_id = kc.kpi_data_id
       AND COALESCE(kc.updated_date, kc.created_date) = last.max_dt
    ) AS q
    """
    return config.spark.read.jdbc(config.DB_URL, q, properties=config.DB_PROPS)
