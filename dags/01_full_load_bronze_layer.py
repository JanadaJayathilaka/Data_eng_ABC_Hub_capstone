from datetime import datetime
from pathlib import Path
import sys

# --------------------------------------------------------------------
# Make sure utils can be imported
# --------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator

from utils.database import extract_data, truncate_table, load_data
from utils.file import read_sql


# --------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------

SOURCE_CONN = "postgres_raw"
TARGET_CONN = "postgres_dw"

SQL_DIR = BASE_DIR / "sql" / "bronze"


# --------------------------------------------------------------------
# Generic Bronze Full Loader
# --------------------------------------------------------------------

def load_bronze_table(sql_file: str, target_table: str):

    sql = read_sql(SQL_DIR / sql_file)

    columns, rows = extract_data(
        conn_id=SOURCE_CONN,
        sql=sql,
    )

    truncate_table(
        conn_id=TARGET_CONN,
        table_name=target_table,
    )

    load_data(
        conn_id=TARGET_CONN,
        table_name=target_table,
        columns=columns,
        rows=rows,
    )

    print(f"Loaded {len(rows)} rows into {target_table}")


# --------------------------------------------------------------------
# DAG
# --------------------------------------------------------------------

default_args = {
    "owner": "pulsetrack",
    "retries": 1,
}


with DAG(
    dag_id="01_full_load_bronze",
    description="One-time full load from Raw into Bronze",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["pulsetrack", "bronze", "full-load", "medallion"],
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    load_account_managers = PythonOperator(
        task_id="load_bronze_account_managers",
        python_callable=load_bronze_table,
        op_kwargs={
            "sql_file": "full_load/account_managers.sql",
            "target_table": "pulsetrack_bronze.account_managers",
        },
    )

    load_advertisers = PythonOperator(
        task_id="load_bronze_advertisers",
        python_callable=load_bronze_table,
        op_kwargs={
            "sql_file": "full_load/advertisers.sql",
            "target_table": "pulsetrack_bronze.advertisers",
        },
    )

    load_partners = PythonOperator(
        task_id="load_bronze_partners",
        python_callable=load_bronze_table,
        op_kwargs={
            "sql_file": "full_load/partners.sql",
            "target_table": "pulsetrack_bronze.partners",
        },
    )

    load_offers = PythonOperator(
        task_id="load_bronze_offers",
        python_callable=load_bronze_table,
        op_kwargs={
            "sql_file": "full_load/offers.sql",
            "target_table": "pulsetrack_bronze.offers",
        },
    )

    load_campaigns = PythonOperator(
        task_id="load_bronze_campaigns",
        python_callable=load_bronze_table,
        op_kwargs={
            "sql_file": "full_load/campaigns.sql",
            "target_table": "pulsetrack_bronze.campaigns",
        },
    )

    load_creatives = PythonOperator(
        task_id="load_bronze_creatives",
        python_callable=load_bronze_table,
        op_kwargs={
            "sql_file": "full_load/creatives.sql",
            "target_table": "pulsetrack_bronze.creatives",
        },
    )

    load_impressions = PythonOperator(
        task_id="load_bronze_impressions",
        python_callable=load_bronze_table,
        op_kwargs={
            "sql_file": "full_load/impressions.sql",
            "target_table": "pulsetrack_bronze.impressions",
        },
    )

    load_clicks = PythonOperator(
        task_id="load_bronze_clicks",
        python_callable=load_bronze_table,
        op_kwargs={
            "sql_file": "full_load/clicks.sql",
            "target_table": "pulsetrack_bronze.clicks",
        },
    )

    load_conversions = PythonOperator(
        task_id="load_bronze_conversions",
        python_callable=load_bronze_table,
        op_kwargs={
            "sql_file": "full_load/conversions.sql",
            "target_table": "pulsetrack_bronze.conversions",
        },
    )

    load_conversion_touchpoints = PythonOperator(
        task_id="load_bronze_conversion_touchpoints",
        python_callable=load_bronze_table,
        op_kwargs={
            "sql_file": "full_load/conversion_touchpoints.sql",
            "target_table": "pulsetrack_bronze.conversion_touchpoints",
        },
    )

    load_revenue_adjustments = PythonOperator(
        task_id="load_bronze_revenue_adjustments",
        python_callable=load_bronze_table,
        op_kwargs={
            "sql_file": "full_load/revenue_adjustments.sql",
            "target_table": "pulsetrack_bronze.revenue_adjustments",
        },
    )

    load_mongo_advertiser_accounts = PythonOperator(
        task_id="load_bronze_mongo_advertiser_accounts",
        python_callable=load_bronze_table,
        op_kwargs={
            "sql_file": "full_load/mongo_advertiser_accounts.sql",
            "target_table": "pulsetrack_bronze.mongo_advertiser_accounts",
        },
    )

    load_mongo_partner_profiles = PythonOperator(
        task_id="load_bronze_mongo_partner_profiles",
        python_callable=load_bronze_table,
        op_kwargs={
            "sql_file": "full_load/mongo_partner_profiles.sql",
            "target_table": "pulsetrack_bronze.mongo_partner_profiles",
        },
    )

    load_kafka_pulsetrack_conversions = PythonOperator(
        task_id="load_bronze_kafka_pulsetrack_conversions",
        python_callable=load_bronze_table,
        op_kwargs={
            "sql_file": "full_load/kafka_pulsetrack_conversions.sql",
            "target_table": "pulsetrack_bronze.kafka_pulsetrack_conversions",
        },
    )

    end = EmptyOperator(
        task_id="end"
    )

    start >> [
        load_account_managers,
        load_advertisers,
        load_partners,
        load_offers,
        load_campaigns,
        load_creatives,
        load_impressions,
        load_clicks,
        load_conversions,
        load_conversion_touchpoints,
        load_revenue_adjustments,
        load_mongo_advertiser_accounts,
        load_mongo_partner_profiles,
        load_kafka_pulsetrack_conversions,
    ] >> end
