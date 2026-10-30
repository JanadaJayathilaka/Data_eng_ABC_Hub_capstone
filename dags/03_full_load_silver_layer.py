from datetime import datetime, timedelta
from pathlib import Path
import sys

# --------------------------------------------------------------------
# Make sure utils can be imported
# --------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

try:
    from airflow.datasets import Dataset
    bronze_full_load_dataset = Dataset("pulsetrack_bronze://full_load")
    silver_full_load_dataset = Dataset("pulsetrack_silver://full_load")
    schedule = [bronze_full_load_dataset]
    outlets = [silver_full_load_dataset]
except Exception:
    schedule = None
    outlets = []

from utils.database import extract_data, load_data, truncate_table
from utils.file import read_sql


# --------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------

SOURCE_CONN = "postgres_dw"
TARGET_CONN = "postgres_dw"

SQL_DIR = BASE_DIR / "sql" / "silver"


# --------------------------------------------------------------------
# Helper Tasks
# --------------------------------------------------------------------

def load_silver_table(sql_file: str, target_table: str):
    """Generic full load function: extract from bronze, clean/transform, truncate silver, and insert."""
    sql = read_sql(SQL_DIR / sql_file)

    columns, rows = extract_data(
        conn_id=SOURCE_CONN,
        sql=sql,
    )

    truncate_table(
        conn_id=TARGET_CONN,
        table_name=target_table,
    )

    if rows:
        load_data(
            conn_id=TARGET_CONN,
            table_name=target_table,
            columns=columns,
            rows=rows,
        )
        print(f"Loaded {len(rows)} cleaned rows into {target_table}")
    else:
        print(f"No rows returned for {target_table}")


# --------------------------------------------------------------------
# DAG Definition
# --------------------------------------------------------------------

default_args = {
    "owner": "pulsetrack",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="03_full_load_silver_layer",
    description="Clean, conform, and deduplicate Bronze data into the Silver layer",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=4,
    default_args=default_args,
    tags=["pulsetrack", "silver", "full-load", "medallion"],
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    # 1. Dimensions / Reference Entities
    load_account_managers = PythonOperator(
        task_id="load_silver_account_managers",
        python_callable=load_silver_table,
        op_kwargs={
            "sql_file": "full_load/account_managers.sql",
            "target_table": "pulsetrack_silver.account_managers",
        },
    )

    load_advertisers = PythonOperator(
        task_id="load_silver_advertisers",
        python_callable=load_silver_table,
        op_kwargs={
            "sql_file": "full_load/advertisers.sql",
            "target_table": "pulsetrack_silver.advertisers",
        },
    )

    load_partners = PythonOperator(
        task_id="load_silver_partners",
        python_callable=load_silver_table,
        op_kwargs={
            "sql_file": "full_load/partners.sql",
            "target_table": "pulsetrack_silver.partners",
        },
    )

    load_offers = PythonOperator(
        task_id="load_silver_offers",
        python_callable=load_silver_table,
        op_kwargs={
            "sql_file": "full_load/offers.sql",
            "target_table": "pulsetrack_silver.offers",
        },
    )

    load_campaigns = PythonOperator(
        task_id="load_silver_campaigns",
        python_callable=load_silver_table,
        op_kwargs={
            "sql_file": "full_load/campaigns.sql",
            "target_table": "pulsetrack_silver.campaigns",
        },
    )

    load_creatives = PythonOperator(
        task_id="load_silver_creatives",
        python_callable=load_silver_table,
        op_kwargs={
            "sql_file": "full_load/creatives.sql",
            "target_table": "pulsetrack_silver.creatives",
        },
    )

    load_mongo_advertiser_accounts = PythonOperator(
        task_id="load_silver_mongo_advertiser_accounts",
        python_callable=load_silver_table,
        op_kwargs={
            "sql_file": "full_load/mongo_advertiser_accounts.sql",
            "target_table": "pulsetrack_silver.mongo_advertiser_accounts",
        },
    )

    load_mongo_partner_profiles = PythonOperator(
        task_id="load_silver_mongo_partner_profiles",
        python_callable=load_silver_table,
        op_kwargs={
            "sql_file": "full_load/mongo_partner_profiles.sql",
            "target_table": "pulsetrack_silver.mongo_partner_profiles",
        },
    )

    # 2. Funnel Events & Facts
    load_impressions = PythonOperator(
        task_id="load_silver_impressions",
        python_callable=load_silver_table,
        op_kwargs={
            "sql_file": "full_load/impressions.sql",
            "target_table": "pulsetrack_silver.impressions",
        },
    )

    load_clicks = PythonOperator(
        task_id="load_silver_clicks",
        python_callable=load_silver_table,
        op_kwargs={
            "sql_file": "full_load/clicks.sql",
            "target_table": "pulsetrack_silver.clicks",
        },
    )

    load_conversions = PythonOperator(
        task_id="load_silver_conversions",
        python_callable=load_silver_table,
        op_kwargs={
            "sql_file": "full_load/conversions.sql",
            "target_table": "pulsetrack_silver.conversions",
        },
    )

    load_conversion_touchpoints = PythonOperator(
        task_id="load_silver_conversion_touchpoints",
        python_callable=load_silver_table,
        op_kwargs={
            "sql_file": "full_load/conversion_touchpoints.sql",
            "target_table": "pulsetrack_silver.conversion_touchpoints",
        },
    )

    load_revenue_adjustments = PythonOperator(
        task_id="load_silver_revenue_adjustments",
        python_callable=load_silver_table,
        op_kwargs={
            "sql_file": "full_load/revenue_adjustments.sql",
            "target_table": "pulsetrack_silver.revenue_adjustments",
        },
    )

    load_kafka_pulsetrack_conversions = PythonOperator(
        task_id="load_silver_kafka_pulsetrack_conversions",
        python_callable=load_silver_table,
        op_kwargs={
            "sql_file": "full_load/kafka_pulsetrack_conversions.sql",
            "target_table": "pulsetrack_silver.kafka_pulsetrack_conversions",
        },
    )

    end = EmptyOperator(
        task_id="end",
        outlets=outlets,
    )

    # --------------------------------------------------------------
    # Task Dependencies (Organized by Referential & Funnel Hierarchy)
    # --------------------------------------------------------------

    # 1. Dimension Hierarchy:
    # Snowflake chain: account_managers -> advertisers -> offers -> campaigns -> creatives
    start >> load_account_managers >> load_advertisers >> load_offers >> load_campaigns >> load_creatives

    # MongoDB CRM Dimensions
    start >> load_mongo_advertiser_accounts
    start >> load_mongo_partner_profiles >> load_partners

    # 2. Funnel Top: Impressions (depends on campaigns, creatives, and conformed partners)
    [load_creatives, load_partners] >> load_impressions

    # 3. Funnel Middle: Clicks (enriches missing attributes & timestamps from impressions)
    load_impressions >> load_clicks

    # 4. Funnel Bottom & Event Streams: Conversions and Kafka conversions
    load_clicks >> [load_conversions, load_kafka_pulsetrack_conversions]

    # 5. Conversion Downstream: Multi-touch attribution touchpoints & revenue adjustments
    load_conversions >> [load_conversion_touchpoints, load_revenue_adjustments]

    # 6. Pipeline Completion
    [
        load_conversion_touchpoints,
        load_revenue_adjustments,
        load_kafka_pulsetrack_conversions,
        load_mongo_advertiser_accounts,
    ] >> end
