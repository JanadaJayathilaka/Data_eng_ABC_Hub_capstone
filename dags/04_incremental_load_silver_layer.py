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

from utils.database import extract_data, upsert_data
from utils.file import read_sql


# --------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------

SOURCE_CONN = "postgres_dw"
TARGET_CONN = "postgres_dw"

SQL_DIR = BASE_DIR / "sql" / "silver"


# --------------------------------------------------------------------
# Get last watermark from Silver
# --------------------------------------------------------------------

def get_last_watermark(target_table: str):
    """Retrieve the high-watermark timestamp from the target Silver table."""
    schema, table = target_table.split(".")

    cols_query = f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = '{schema}' AND table_name = '{table}';
    """
    _, rows = extract_data(conn_id=TARGET_CONN, sql=cols_query)
    col_names = {r[0] for r in rows}

    if "updated_at" in col_names and "created_at" in col_names:
        watermark_expr = "COALESCE(GREATEST(MAX(created_at), MAX(updated_at)), '1970-01-01 00:00:00'::timestamp)::text"
    elif "message_timestamp" in col_names:
        watermark_expr = "COALESCE(MAX(message_timestamp), '1970-01-01 00:00:00'::timestamp)::text"
    elif "updated_at" in col_names:
        watermark_expr = "COALESCE(MAX(updated_at), '1970-01-01 00:00:00'::timestamp)::text"
    elif "created_at" in col_names:
        watermark_expr = "COALESCE(MAX(created_at), '1970-01-01 00:00:00'::timestamp)::text"
    else:
        return "1970-01-01 00:00:00"

    sql = f"SELECT {watermark_expr} AS last_watermark FROM {target_table};"
    _, res = extract_data(conn_id=TARGET_CONN, sql=sql)
    last_watermark = res[0][0] if res and res[0] and res[0][0] else "1970-01-01 00:00:00"

    print(f"Last watermark for {target_table}: {last_watermark}")
    return str(last_watermark)


# --------------------------------------------------------------------
# Generic Silver Incremental Loader (Extract + Transform + UPSERT)
# --------------------------------------------------------------------

def load_silver_incremental_table(sql_file: str, target_table: str, conflict_columns: list):
    """Extract new/updated Bronze records above watermark, clean, and upsert into Silver."""
    last_watermark = get_last_watermark(target_table)

    sql = read_sql(SQL_DIR / sql_file)
    sql = sql.replace("{{ last_watermark }}", str(last_watermark))

    columns, rows = extract_data(
        conn_id=SOURCE_CONN,
        sql=sql,
    )

    if rows:
        upsert_data(
            conn_id=TARGET_CONN,
            table_name=target_table,
            columns=columns,
            rows=rows,
            conflict_columns=conflict_columns,
        )
        print(f"Successfully upserted {len(rows)} incremental rows into {target_table}")
    else:
        print(f"No new or updated rows found for {target_table} past watermark {last_watermark}")


# --------------------------------------------------------------------
# DAG Definition
# --------------------------------------------------------------------

default_args = {
    "owner": "pulsetrack",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="04_incremental_load_silver_layer",
    description="Incrementally clean, conform, and upsert Bronze data into the Silver layer",
    start_date=datetime(2026, 1, 1),
    schedule="30 0 * * *",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=4,
    default_args=default_args,
    tags=["pulsetrack", "silver", "incremental", "medallion"],
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    # 1. Dimensions / Reference Entities
    load_account_managers = PythonOperator(
        task_id="load_silver_account_managers",
        python_callable=load_silver_incremental_table,
        op_kwargs={
            "sql_file": "incremental_load/account_managers.sql",
            "target_table": "pulsetrack_silver.account_managers",
            "conflict_columns": ["am_id"],
        },
    )

    load_advertisers = PythonOperator(
        task_id="load_silver_advertisers",
        python_callable=load_silver_incremental_table,
        op_kwargs={
            "sql_file": "incremental_load/advertisers.sql",
            "target_table": "pulsetrack_silver.advertisers",
            "conflict_columns": ["advertiser_id"],
        },
    )

    load_partners = PythonOperator(
        task_id="load_silver_partners",
        python_callable=load_silver_incremental_table,
        op_kwargs={
            "sql_file": "incremental_load/partners.sql",
            "target_table": "pulsetrack_silver.partners",
            "conflict_columns": ["partner_id"],
        },
    )

    load_offers = PythonOperator(
        task_id="load_silver_offers",
        python_callable=load_silver_incremental_table,
        op_kwargs={
            "sql_file": "incremental_load/offers.sql",
            "target_table": "pulsetrack_silver.offers",
            "conflict_columns": ["offer_id"],
        },
    )

    load_campaigns = PythonOperator(
        task_id="load_silver_campaigns",
        python_callable=load_silver_incremental_table,
        op_kwargs={
            "sql_file": "incremental_load/campaigns.sql",
            "target_table": "pulsetrack_silver.campaigns",
            "conflict_columns": ["campaign_id"],
        },
    )

    load_creatives = PythonOperator(
        task_id="load_silver_creatives",
        python_callable=load_silver_incremental_table,
        op_kwargs={
            "sql_file": "incremental_load/creatives.sql",
            "target_table": "pulsetrack_silver.creatives",
            "conflict_columns": ["creative_id"],
        },
    )

    load_mongo_advertiser_accounts = PythonOperator(
        task_id="load_silver_mongo_advertiser_accounts",
        python_callable=load_silver_incremental_table,
        op_kwargs={
            "sql_file": "incremental_load/mongo_advertiser_accounts.sql",
            "target_table": "pulsetrack_silver.mongo_advertiser_accounts",
            "conflict_columns": ["advertiser_id", "am_id", "valid_from"],
        },
    )

    load_mongo_partner_profiles = PythonOperator(
        task_id="load_silver_mongo_partner_profiles",
        python_callable=load_silver_incremental_table,
        op_kwargs={
            "sql_file": "incremental_load/mongo_partner_profiles.sql",
            "target_table": "pulsetrack_silver.mongo_partner_profiles",
            "conflict_columns": ["partner_id"],
        },
    )

    # 2. Funnel Events & Facts
    load_impressions = PythonOperator(
        task_id="load_silver_impressions",
        python_callable=load_silver_incremental_table,
        op_kwargs={
            "sql_file": "incremental_load/impressions.sql",
            "target_table": "pulsetrack_silver.impressions",
            "conflict_columns": ["impression_id"],
        },
    )

    load_clicks = PythonOperator(
        task_id="load_silver_clicks",
        python_callable=load_silver_incremental_table,
        op_kwargs={
            "sql_file": "incremental_load/clicks.sql",
            "target_table": "pulsetrack_silver.clicks",
            "conflict_columns": ["click_id"],
        },
    )

    load_conversions = PythonOperator(
        task_id="load_silver_conversions",
        python_callable=load_silver_incremental_table,
        op_kwargs={
            "sql_file": "incremental_load/conversions.sql",
            "target_table": "pulsetrack_silver.conversions",
            "conflict_columns": ["conversion_id"],
        },
    )

    load_conversion_touchpoints = PythonOperator(
        task_id="load_silver_conversion_touchpoints",
        python_callable=load_silver_incremental_table,
        op_kwargs={
            "sql_file": "incremental_load/conversion_touchpoints.sql",
            "target_table": "pulsetrack_silver.conversion_touchpoints",
            "conflict_columns": ["touchpoint_id"],
        },
    )

    load_revenue_adjustments = PythonOperator(
        task_id="load_silver_revenue_adjustments",
        python_callable=load_silver_incremental_table,
        op_kwargs={
            "sql_file": "incremental_load/revenue_adjustments.sql",
            "target_table": "pulsetrack_silver.revenue_adjustments",
            "conflict_columns": ["adjustment_id"],
        },
    )

    load_kafka_pulsetrack_conversions = PythonOperator(
        task_id="load_silver_kafka_pulsetrack_conversions",
        python_callable=load_silver_incremental_table,
        op_kwargs={
            "sql_file": "incremental_load/kafka_pulsetrack_conversions.sql",
            "target_table": "pulsetrack_silver.kafka_pulsetrack_conversions",
            "conflict_columns": ["event_id"],
        },
    )

    end = EmptyOperator(
        task_id="end"
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
