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
    dm_dataset = Dataset("pulsetrack_dm://advertiser_performance")
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
# Helper Tasks
# --------------------------------------------------------------------

def init_datamart_schema_and_tables():
    """
    Initializes the pulsetrack_dm schema, conformed dimension views,
    fact table, indexes, and analytical KPI views.
    """
    sql = read_sql(SQL_DIR / "00_create_advertiser_datamart.sql")
    conn = get_connection(TARGET_CONN)
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        conn.commit()
        print("Successfully initialized pulsetrack_dm schema and views.")
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()


def load_advertiser_performance_fact():
    """
    Populates pulsetrack_dm.dm_fact_advertiser_performance_daily by pre-aggregating
    EDW Gold facts (fact_impression, fact_click, fact_conversion) safely.
    """
    conn = get_connection(TARGET_CONN)
    cursor = conn.cursor()
    try:
        # Atomic truncate before full load
        cursor.execute("TRUNCATE TABLE pulsetrack_dm.dm_fact_advertiser_performance_daily RESTART IDENTITY;")
        
        load_sql = read_sql(SQL_DIR / "01_load_advertiser_performance_datamart.sql")
        insert_sql = f"""
            INSERT INTO pulsetrack_dm.dm_fact_advertiser_performance_daily (
                date_key,
                advertiser_key,
                account_manager_key,
                impressions,
                clicks,
                conversions,
                revenue,
                net_revenue,
                payout,
                created_at,
                updated_at
            )
            {load_sql}
        """
        cursor.execute(insert_sql)
        row_count = cursor.rowcount
        conn.commit()
        print(f"Successfully loaded {row_count} rows into pulsetrack_dm.dm_fact_advertiser_performance_daily")
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
    description="Build and populate the PulseTrack Advertiser Performance Data Mart from the Enterprise Data Warehouse",
    start_date=datetime(2026, 1, 1),
    schedule=schedule,
    catchup=False,
    max_active_tasks=2,
    default_args=default_args,
    tags=["pulsetrack", "datamart", "advertiser-performance", "kpis"],
) as dag:

    start = EmptyOperator(task_id="start")

    init_schema = PythonOperator(
        task_id="init_datamart_schema_and_tables",
        python_callable=init_datamart_schema_and_tables,
    )

    load_fact = PythonOperator(
        task_id="load_advertiser_performance_fact",
        python_callable=load_advertiser_performance_fact,
        outlets=outlets,
    )

    end = EmptyOperator(task_id="end")

    start >> init_schema >> load_fact >> end

