from datetime import datetime
from pathlib import Path
import sys

# --------------------------------------------------------------------
# Make sure the dags folder (or project root) is on the path
# BEFORE importing anything from utils
# --------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from utils.database import extract_data, load_data
from utils.file import read_sql


# --------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------

SOURCE_CONN = "postgres_raw"
TARGET_CONN = "postgres_dw"

SQL_DIR = BASE_DIR / "sql" / "bronze"


# --------------------------------------------------------------------
# Get last watermark from Bronze
# --------------------------------------------------------------------

def get_last_watermark(target_table: str):
    schema, table = target_table.split(".")

    cols_query = f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = '{schema}' AND table_name = '{table}';
    """
    _, rows = extract_data(conn_id=TARGET_CONN, sql=cols_query)
    col_names = {r[0] for r in rows}

    if "updated_at" in col_names and "created_at" in col_names:
        watermark_expr = "COALESCE(GREATEST(MAX(created_at::text), MAX(updated_at::text)), '1970-01-01 00:00:00')"
    elif "extracted_at" in col_names:
        watermark_expr = "COALESCE(MAX(extracted_at::text), '1970-01-01 00:00:00')"
    elif "message_timestamp" in col_names:
        watermark_expr = "COALESCE(MAX(message_timestamp::text), '1970-01-01 00:00:00')"
    elif "updated_at" in col_names:
        watermark_expr = "COALESCE(MAX(updated_at::text), '1970-01-01 00:00:00')"
    elif "created_at" in col_names:
        watermark_expr = "COALESCE(MAX(created_at::text), '1970-01-01 00:00:00')"
    else:
        return "1970-01-01 00:00:00"

    sql = f"SELECT {watermark_expr} AS last_watermark FROM {target_table};"
    _, res = extract_data(conn_id=TARGET_CONN, sql=sql)
    last_watermark = res[0][0] if res and res[0] and res[0][0] else "1970-01-01 00:00:00"

    # Fallback if table is currently empty
    if not last_watermark or str(last_watermark).strip() in ("", "None"):
        last_watermark = "1970-01-01 00:00:00"

    print(f"Last watermark for {target_table}: {last_watermark}")
    return str(last_watermark)


# --------------------------------------------------------------------
# Generic Bronze Incremental Loader
# --------------------------------------------------------------------

def load_bronze_table(sql_file: str, target_table: str):

    # --------------------------------------------------------------
    # Get the last successfully loaded timestamp from Bronze
    # --------------------------------------------------------------

    last_watermark = get_last_watermark(target_table)

    # --------------------------------------------------------------
    # Read incremental SQL template
    # --------------------------------------------------------------

    sql = read_sql(SQL_DIR / sql_file)

    # --------------------------------------------------------------
    # Replace watermark placeholder
    # --------------------------------------------------------------

    sql = sql.replace(
        "{{ last_watermark }}",
        str(last_watermark)
    )

    # --------------------------------------------------------------
    # Extract only new / changed records
    # --------------------------------------------------------------

    columns, rows = extract_data(
        conn_id=SOURCE_CONN,
        sql=sql,
    )

    # --------------------------------------------------------------
    # Append to Bronze
    # --------------------------------------------------------------

    if rows:

        load_data(
            conn_id=TARGET_CONN,
            table_name=target_table,
            columns=columns,
            rows=rows,
        )

        print(
            f"Loaded {len(rows)} incremental rows "
            f"into {target_table}"
        )

    else:

        print(
            f"No new or updated rows found "
            f"for {target_table}"
        )


# --------------------------------------------------------------------
# DAG configuration
# --------------------------------------------------------------------

default_args = {
    "owner": "pulsetrack",
    "retries": 1,
}


# --------------------------------------------------------------------
# DAG
# --------------------------------------------------------------------

with DAG(
    dag_id="02_incremental_load_bronze",
    description="Incremental load from Raw into Bronze",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["pulsetrack", "bronze", "incremental", "medallion"],
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    load_account_managers = PythonOperator(
        task_id="load_bronze_account_managers",
        python_callable=load_bronze_table,
        op_kwargs={
            "sql_file": "incremental_load/account_managers.sql",
            "target_table": "pulsetrack_bronze.account_managers",
        },
    )

    load_advertisers = PythonOperator(
        task_id="load_bronze_advertisers",
        python_callable=load_bronze_table,
        op_kwargs={
            "sql_file": "incremental_load/advertisers.sql",
            "target_table": "pulsetrack_bronze.advertisers",
        },
    )

    load_partners = PythonOperator(
        task_id="load_bronze_partners",
        python_callable=load_bronze_table,
        op_kwargs={
            "sql_file": "incremental_load/partners.sql",
            "target_table": "pulsetrack_bronze.partners",
        },
    )

    load_offers = PythonOperator(
        task_id="load_bronze_offers",
        python_callable=load_bronze_table,
        op_kwargs={
            "sql_file": "incremental_load/offers.sql",
            "target_table": "pulsetrack_bronze.offers",
        },
    )

    load_campaigns = PythonOperator(
        task_id="load_bronze_campaigns",
        python_callable=load_bronze_table,
        op_kwargs={
            "sql_file": "incremental_load/campaigns.sql",
            "target_table": "pulsetrack_bronze.campaigns",
        },
    )

    load_creatives = PythonOperator(
        task_id="load_bronze_creatives",
        python_callable=load_bronze_table,
        op_kwargs={
            "sql_file": "incremental_load/creatives.sql",
            "target_table": "pulsetrack_bronze.creatives",
        },
    )

    load_impressions = PythonOperator(
        task_id="load_bronze_impressions",
        python_callable=load_bronze_table,
        op_kwargs={
            "sql_file": "incremental_load/impressions.sql",
            "target_table": "pulsetrack_bronze.impressions",
        },
    )

    load_clicks = PythonOperator(
        task_id="load_bronze_clicks",
        python_callable=load_bronze_table,
        op_kwargs={
            "sql_file": "incremental_load/clicks.sql",
            "target_table": "pulsetrack_bronze.clicks",
        },
    )

    load_conversions = PythonOperator(
        task_id="load_bronze_conversions",
        python_callable=load_bronze_table,
        op_kwargs={
            "sql_file": "incremental_load/conversions.sql",
            "target_table": "pulsetrack_bronze.conversions",
        },
    )

    load_conversion_touchpoints = PythonOperator(
        task_id="load_bronze_conversion_touchpoints",
        python_callable=load_bronze_table,
        op_kwargs={
            "sql_file": "incremental_load/conversion_touchpoints.sql",
            "target_table": "pulsetrack_bronze.conversion_touchpoints",
        },
    )

    load_revenue_adjustments = PythonOperator(
        task_id="load_bronze_revenue_adjustments",
        python_callable=load_bronze_table,
        op_kwargs={
            "sql_file": "incremental_load/revenue_adjustments.sql",
            "target_table": "pulsetrack_bronze.revenue_adjustments",
        },
    )

    load_mongo_advertiser_accounts = PythonOperator(
        task_id="load_bronze_mongo_advertiser_accounts",
        python_callable=load_bronze_table,
        op_kwargs={
            "sql_file": "incremental_load/mongo_advertiser_accounts.sql",
            "target_table": "pulsetrack_bronze.mongo_advertiser_accounts",
        },
    )

    load_mongo_partner_profiles = PythonOperator(
        task_id="load_bronze_mongo_partner_profiles",
        python_callable=load_bronze_table,
        op_kwargs={
            "sql_file": "incremental_load/mongo_partner_profiles.sql",
            "target_table": "pulsetrack_bronze.mongo_partner_profiles",
        },
    )

    load_kafka_pulsetrack_conversions = PythonOperator(
        task_id="load_bronze_kafka_pulsetrack_conversions",
        python_callable=load_bronze_table,
        op_kwargs={
            "sql_file": "incremental_load/kafka_pulsetrack_conversions.sql",
            "target_table": "pulsetrack_bronze.kafka_pulsetrack_conversions",
        },
    )

    end = EmptyOperator(
        task_id="end"
    )

    # --------------------------------------------------------------
    # Parallel execution
    # --------------------------------------------------------------

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
