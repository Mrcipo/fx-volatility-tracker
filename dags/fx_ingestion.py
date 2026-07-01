from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

PYTHON = "/home/airflow/.local/bin/python"
AIRFLOW_HOME = "/opt/airflow"

ENV = {
    "PYTHONPATH": AIRFLOW_HOME,
    "DBT_PROFILES_DIR": f"{AIRFLOW_HOME}/dbt/profiles",
    "PATH": "/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin",
    "BMX_TOKEN": "{{ var.value.get('BMX_TOKEN', '') }}",
    "POSTGRES_HOST": "postgres",
    "POSTGRES_PORT": "5432",
    "POSTGRES_USER": "fxuser",
    "POSTGRES_PASSWORD": "fxpass",
    "POSTGRES_DB": "fxdb",
}

default_args = {
    "owner": "fx-team",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "email_on_failure": False,
}

with DAG(
    dag_id="fx_ingestion",
    description="Pipeline FX: extracción, carga raw y transformación dbt",
    schedule_interval="0 */2 * * *",
    start_date=datetime(2026, 6, 23),
    catchup=False,
    default_args=default_args,
    tags=["fx", "ingestion", "dbt"],
) as dag:

    extract_bcra = BashOperator(
        task_id="extract_bcra",
        bash_command=f"cd {AIRFLOW_HOME} && {PYTHON} -m extractors.bcra",
        env=ENV,
    )

    extract_dolar = BashOperator(
        task_id="extract_dolar",
        bash_command=f"cd {AIRFLOW_HOME} && {PYTHON} -m extractors.dolarapi",
        env=ENV,
    )

    extract_banxico = BashOperator(
        task_id="extract_banxico",
        bash_command=f"cd {AIRFLOW_HOME} && {PYTHON} -m extractors.banxico",
        env=ENV,
    )

    load_bcra_variables = BashOperator(
        task_id="load_bcra_variables",
        bash_command=f"cd {AIRFLOW_HOME} && {PYTHON} -m loaders.load_raw bcra_variables",
        env=ENV,
    )

    load_bcra_tipo_cambio = BashOperator(
        task_id="load_bcra_tipo_cambio",
        bash_command=f"cd {AIRFLOW_HOME} && {PYTHON} -m loaders.load_raw bcra_tipo_cambio",
        env=ENV,
    )

    load_dolarapi = BashOperator(
        task_id="load_dolarapi",
        bash_command=f"cd {AIRFLOW_HOME} && {PYTHON} -m loaders.load_raw dolarapi",
        env=ENV,
    )

    load_banxico = BashOperator(
        task_id="load_banxico",
        bash_command=f"cd {AIRFLOW_HOME} && {PYTHON} -m loaders.load_raw banxico",
        env=ENV,
    )

    dbt_staging = BashOperator(
        task_id="dbt_staging",
        bash_command=(
            f"cd {AIRFLOW_HOME}/dbt/fx_volatility && "
            "dbt run --select staging --profiles-dir "
            f"{AIRFLOW_HOME}/dbt/profiles"
        ),
        env=ENV,
        trigger_rule="all_done",
    )

    dbt_marts = BashOperator(
        task_id="dbt_marts",
        bash_command=(
            f"cd {AIRFLOW_HOME}/dbt/fx_volatility && "
            "dbt run --select marts --profiles-dir "
            f"{AIRFLOW_HOME}/dbt/profiles"
        ),
        env=ENV,
    )

    # Dependencias
    extract_bcra >> [load_bcra_variables, load_bcra_tipo_cambio]
    extract_dolar >> load_dolarapi
    extract_banxico >> load_banxico
    [load_bcra_variables, load_bcra_tipo_cambio,
     load_dolarapi, load_banxico] >> dbt_staging
    dbt_staging >> dbt_marts
