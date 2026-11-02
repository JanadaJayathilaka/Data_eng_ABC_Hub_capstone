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
    silver_full_load_dataset = Dataset("pulsetrack_silver://full_load")
    gold_full_load_dataset = Dataset("pulsetrack_gold://full_load")
    schedule = [silver_full_load_dataset]
    outlets = [gold_full_load_dataset]
except Exception:
    schedule = None
    outlets = []

from utils.database import get_connection
from utils.file import read_sql


# --------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------

TARGET_CONN = "postgres_dw"
SQL_DIR = BASE_DIR / "sql" / "gold"


# --------------------------------------------------------------------
# Helper Tasks
# --------------------------------------------------------------------

def truncate_gold_layer():
    """
    Truncate all Gold tables atomically at the start of the full load pipeline.
    Doing this in a single atomic transaction prevents concurrent CASCADE deadlocks
    across parallel dimension and fact load tasks.
    """
    tables = [
        "pulsetrack_gold.bridge_conversion_touchpoint",
        "pulsetrack_gold.fact_conversion",
        "pulsetrack_gold.fact_click",
        "pulsetrack_gold.fact_impression",
        "pulsetrack_gold.dim_creative",
        "pulsetrack_gold.dim_campaign",
        "pulsetrack_gold.dim_offer",
        "pulsetrack_gold.dim_advertiser",
        "pulsetrack_gold.dim_partner",
        "pulsetrack_gold.dim_account_manager",
        "pulsetrack_gold.dim_device",
        "pulsetrack_gold.dim_geo",
        "pulsetrack_gold.dim_date",
    ]
    conn = get_connection(TARGET_CONN)
    cursor = conn.cursor()
    try:
        cursor.execute(f"TRUNCATE TABLE {', '.join(tables)} RESTART IDENTITY CASCADE;")
        conn.commit()
        print("Successfully truncated all Gold layer tables.")
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()


def load_gold_table(sql_file: str, target_table: str):
    """
    Generic full load function for Gold layer:
    Reads transform SQL and executes direct in-database ELT
    for maximum performance and transactional consistency.
    """
    sql = read_sql(SQL_DIR / sql_file)

    conn = get_connection(TARGET_CONN)
    cursor = conn.cursor()

    try:
        insert_sql = f"INSERT INTO {target_table} {sql}"
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
    dag_id="05_full_load_gold_layer",
    description="Build Kimball Star Schema (Dimensions, Facts, Bridge) into the Gold layer",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=4,
    default_args=default_args,
    tags=["pulsetrack", "gold", "full-load", "kimball", "star-schema"],
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    truncate_gold = PythonOperator(
        task_id="truncate_gold_layer",
        python_callable=truncate_gold_layer,
    )

    # ----------------------------------------------------------------
    # 1. Dimensions (Conformed Dimensions & SCD Type 2)
    # ----------------------------------------------------------------
    load_dim_date = PythonOperator(
        task_id="load_gold_dim_date",
        python_callable=load_gold_table,
        op_kwargs={
            "sql_file": "full_load/dim_date.sql",
            "target_table": "pulsetrack_gold.dim_date",
        },
    )

    load_dim_geo = PythonOperator(
        task_id="load_gold_dim_geo",
        python_callable=load_gold_table,
        op_kwargs={
            "sql_file": "full_load/dim_geo.sql",
            "target_table": "pulsetrack_gold.dim_geo",
        },
    )

    load_dim_device = PythonOperator(
        task_id="load_gold_dim_device",
        python_callable=load_gold_table,
        op_kwargs={
            "sql_file": "full_load/dim_device.sql",
            "target_table": "pulsetrack_gold.dim_device",
        },
    )

    load_dim_account_manager = PythonOperator(
        task_id="load_gold_dim_account_manager",
        python_callable=load_gold_table,
        op_kwargs={
            "sql_file": "full_load/dim_account_manager.sql",
            "target_table": "pulsetrack_gold.dim_account_manager",
        },
    )

    load_dim_advertiser = PythonOperator(
        task_id="load_gold_dim_advertiser",
        python_callable=load_gold_table,
        op_kwargs={
            "sql_file": "full_load/dim_advertiser.sql",
            "target_table": "pulsetrack_gold.dim_advertiser",
        },
    )

    load_dim_partner = PythonOperator(
        task_id="load_gold_dim_partner",
        python_callable=load_gold_table,
        op_kwargs={
            "sql_file": "full_load/dim_partner.sql",
            "target_table": "pulsetrack_gold.dim_partner",
        },
    )

    load_dim_offer = PythonOperator(
        task_id="load_gold_dim_offer",
        python_callable=load_gold_table,
        op_kwargs={
            "sql_file": "full_load/dim_offer.sql",
            "target_table": "pulsetrack_gold.dim_offer",
        },
    )

    load_dim_campaign = PythonOperator(
        task_id="load_gold_dim_campaign",
        python_callable=load_gold_table,
        op_kwargs={
            "sql_file": "full_load/dim_campaign.sql",
            "target_table": "pulsetrack_gold.dim_campaign",
        },
    )

    load_dim_creative = PythonOperator(
        task_id="load_gold_dim_creative",
        python_callable=load_gold_table,
        op_kwargs={
            "sql_file": "full_load/dim_creative.sql",
            "target_table": "pulsetrack_gold.dim_creative",
        },
    )

    # ----------------------------------------------------------------
    # 2. Fact Tables (Event and Transaction Grain)
    # ----------------------------------------------------------------
    load_fact_impression = PythonOperator(
        task_id="load_gold_fact_impression",
        python_callable=load_gold_table,
        op_kwargs={
            "sql_file": "full_load/fact_impression.sql",
            "target_table": "pulsetrack_gold.fact_impression",
        },
    )

    load_fact_click = PythonOperator(
        task_id="load_gold_fact_click",
        python_callable=load_gold_table,
        op_kwargs={
            "sql_file": "full_load/fact_click.sql",
            "target_table": "pulsetrack_gold.fact_click",
        },
    )

    load_fact_conversion = PythonOperator(
        task_id="load_gold_fact_conversion",
        python_callable=load_gold_table,
        op_kwargs={
            "sql_file": "full_load/fact_conversion.sql",
            "target_table": "pulsetrack_gold.fact_conversion",
        },
    )

    # ----------------------------------------------------------------
    # 3. Bridge Table (Multi-Touch Attribution)
    # ----------------------------------------------------------------
    load_bridge_conversion_touchpoint = PythonOperator(
        task_id="load_gold_bridge_conversion_touchpoint",
        python_callable=load_gold_table,
        op_kwargs={
            "sql_file": "full_load/bridge_conversion_touchpoint.sql",
            "target_table": "pulsetrack_gold.bridge_conversion_touchpoint",
        },
    )

    end = EmptyOperator(
        task_id="end",
        outlets=outlets,
    )

    # ----------------------------------------------------------------
    # Task Dependencies (Kimball Star Schema Hierarchy)
    # ----------------------------------------------------------------

    # 1. Conformed dimension setup
    # Truncate all tables first, then start independent dimensions in parallel
    start >> truncate_gold >> [
        load_dim_date,
        load_dim_geo,
        load_dim_device,
        load_dim_account_manager,
        load_dim_partner,
    ]

    # Dimension relationship hierarchy:
    # Account Manager -> Advertiser -> Offer -> Campaign -> Creative
    load_dim_account_manager >> load_dim_advertiser >> load_dim_offer >> load_dim_campaign >> load_dim_creative

    all_dimensions = [
        load_dim_date,
        load_dim_geo,
        load_dim_device,
        load_dim_account_manager,
        load_dim_advertiser,
        load_dim_partner,
        load_dim_offer,
        load_dim_campaign,
        load_dim_creative,
    ]

    # 2. Fact tables depend on all conformed dimensions being fully loaded
    all_dimensions >> load_fact_impression
    all_dimensions >> load_fact_click
    all_dimensions >> load_fact_conversion

    # 3. Multi-Touch Attribution Bridge table links clicks and conversions
    [load_fact_click, load_fact_conversion] >> load_bridge_conversion_touchpoint

    # 4. Pipeline completion
    [load_fact_impression, load_bridge_conversion_touchpoint] >> end
