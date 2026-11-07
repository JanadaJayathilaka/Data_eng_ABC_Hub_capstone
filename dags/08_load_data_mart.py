from datetime import datetime, timedelta
from pathlib import Path
import sys

# --------------------------------------------------------------------
# Make sure utils can be imported
# --------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

from airflow import DAG
try:
    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.empty import EmptyOperator
    from airflow.operators.python import PythonOperator

try:
    from airflow.datasets import Dataset
    gold_full_load_dataset = Dataset("pulsetrack_gold://full_load")
    dm_dataset = Dataset("pulsetrack_dm://full_load")
    schedule = [gold_full_load_dataset]
    outlets = [dm_dataset]
except Exception:
    schedule = None
    outlets = []

from utils.database import get_connection
from utils.file import read_sql


# --------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------

TARGET_CONN = "postgres_dw"
SQL_DIR = BASE_DIR / "sql" / "datamart"


# --------------------------------------------------------------------
# Helper Tasks (Full Load only - no incremental logic)
# --------------------------------------------------------------------

def init_datamart_schema_and_tables():
    """
    Initializes the pulsetrack_dm schema, conformed dimension views,
    fact tables, performance indexes, and business presentation views.
    """
    sql = read_sql(SQL_DIR / "00_create_datamart_schema_and_tables.sql")
    conn = get_connection(TARGET_CONN)
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        conn.commit()
        print("Successfully initialized pulsetrack_dm schema, tables, and views.")
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()


def truncate_datamart_facts():
    """
    Atomically truncates all 3 Data Mart fact tables before full load.
    """
    tables = [
        "pulsetrack_dm.dm_fact_marketing_performance_daily",
        "pulsetrack_dm.dm_fact_partner_attribution_daily",
        "pulsetrack_dm.dm_fact_account_manager_performance_daily",
    ]
    conn = get_connection(TARGET_CONN)
    cursor = conn.cursor()
    try:
        cursor.execute(f"TRUNCATE TABLE {', '.join(tables)} RESTART IDENTITY;")
        conn.commit()
        print("Successfully truncated all Data Mart fact tables.")
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()


def load_datamart_fact(sql_file: str, target_table: str, insert_cols: list):
    """
    Executes high-performance in-database ELT to load aggregated Data Mart fact tables.
    """
    select_sql = read_sql(SQL_DIR / sql_file)
    cols_str = ", ".join(insert_cols)
    insert_sql = f"INSERT INTO {target_table} ({cols_str}) {select_sql}"

    conn = get_connection(TARGET_CONN)
    cursor = conn.cursor()
    try:
        cursor.execute(insert_sql)
        row_count = cursor.rowcount
        conn.commit()
        print(f"Successfully loaded {row_count} rows into {target_table}")
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()


# --------------------------------------------------------------------
# DAG Definition
# --------------------------------------------------------------------

default_args = {
    "owner": "pulsetrack",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="08_load_data_mart",
    description="Full Load Pipeline: Build and populate the PulseTrack Multi-Fact Data Mart from the Gold EDW",
    start_date=datetime(2026, 1, 1),
    schedule=schedule,
    catchup=False,
    max_active_tasks=3,
    default_args=default_args,
    tags=["pulsetrack", "datamart", "full-load", "kimball", "kpis"],
) as dag:

    start = EmptyOperator(task_id="start")

    init_schema = PythonOperator(
        task_id="init_datamart_schema_and_tables",
        python_callable=init_datamart_schema_and_tables,
    )

    truncate_facts = PythonOperator(
        task_id="truncate_datamart_facts",
        python_callable=truncate_datamart_facts,
    )

    # 1. Load Marketing Performance Fact
    load_marketing_fact = PythonOperator(
        task_id="load_marketing_performance_fact",
        python_callable=load_datamart_fact,
        op_kwargs={
            "sql_file": "01_load_marketing_performance_fact.sql",
            "target_table": "pulsetrack_dm.dm_fact_marketing_performance_daily",
            "insert_cols": [
                "date_key", "advertiser_key", "offer_key", "campaign_key", "creative_key",
                "partner_key", "account_manager_key", "geo_key", "device_key",
                "impressions", "clicks", "conversions", "revenue", "adjusted_revenue",
                "payout", "margin", "created_at"
            ],
        },
    )

    # 2. Load Partner Attribution Fact
    load_partner_fact = PythonOperator(
        task_id="load_partner_attribution_fact",
        python_callable=load_datamart_fact,
        op_kwargs={
            "sql_file": "02_load_partner_attribution_fact.sql",
            "target_table": "pulsetrack_dm.dm_fact_partner_attribution_daily",
            "insert_cols": [
                "date_key", "partner_key", "campaign_key",
                "total_clicks", "direct_conversions", "attributed_conversions",
                "attributed_revenue", "created_at"
            ],
        },
    )

    # 3. Load Account Manager Performance Fact
    load_am_fact = PythonOperator(
        task_id="load_account_manager_fact",
        python_callable=load_datamart_fact,
        op_kwargs={
            "sql_file": "03_load_account_manager_fact.sql",
            "target_table": "pulsetrack_dm.dm_fact_account_manager_performance_daily",
            "insert_cols": [
                "date_key", "account_manager_key", "advertiser_key",
                "conversions", "revenue", "adjusted_revenue", "payout",
                "margin", "active_campaigns_count", "created_at"
            ],
        },
    )

    end = EmptyOperator(
        task_id="end",
        outlets=outlets,
    )

    # Orchestration Flow: Schema -> Truncate -> Parallel Fact Loads -> End
    start >> init_schema >> truncate_facts >> [load_marketing_fact, load_partner_fact, load_am_fact] >> end
