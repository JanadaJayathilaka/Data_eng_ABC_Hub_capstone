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
    silver_inc_dataset = Dataset("pulsetrack_silver://incremental")
    gold_inc_dataset = Dataset("pulsetrack_gold://incremental")
    schedule = [silver_inc_dataset]
    outlets = [gold_inc_dataset]
except Exception:
    schedule = None
    outlets = []

from utils.database import get_connection
from utils.file import read_sql


# --------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------

TARGET_CONN = "postgres_dw"
SQL_DIR = BASE_DIR / "sql" / "gold" / "incremental_load"


# --------------------------------------------------------------------
# Watermark Helper
# --------------------------------------------------------------------

def get_gold_fact_watermark(gold_table: str, date_col: str = "date_key") -> str:
    """Retrieve the high-watermark date from Gold fact table joined with dim_date."""
    sql = f"""
        SELECT COALESCE(MAX(dd.full_date), '1970-01-01'::date)::text
        FROM {gold_table} f
        JOIN pulsetrack_gold.dim_date dd ON f.{date_col} = dd.date_key
        WHERE f.{date_col} > 0;
    """
    conn = get_connection(TARGET_CONN)
    cursor = conn.cursor()
    cursor.execute(sql)
    res = cursor.fetchone()
    cursor.close()
    conn.close()

    watermark = res[0] if res and res[0] else "1970-01-01"
    watermark_str = f"{watermark} 00:00:00" if len(str(watermark)) == 10 else str(watermark)
    print(f"High-watermark for {gold_table}: {watermark_str}")
    return watermark_str


# --------------------------------------------------------------------
# Incremental Loader Helper
# --------------------------------------------------------------------

def load_gold_incremental(
    sql_file: str,
    target_table: str,
    target_columns: list = None,
    watermark_table: str = None,
    date_col: str = "date_key",
):
    """
    Generic incremental loader function for Gold layer:
    Reads transform SQL, injects watermark (if applicable), and executes
    direct in-database ELT with ON CONFLICT resolution for idempotency.
    """
    sql = read_sql(SQL_DIR / sql_file)

    if watermark_table:
        watermark = get_gold_fact_watermark(watermark_table, date_col=date_col)
        sql = sql.replace("{{ last_watermark }}", str(watermark))

    conn = get_connection(TARGET_CONN)
    cursor = conn.cursor()

    try:
        if target_columns:
            cols_clause = f"({', '.join(target_columns)})"
            insert_sql = f"INSERT INTO {target_table} {cols_clause} {sql}"
        else:
            insert_sql = f"INSERT INTO {target_table} {sql}"

        cursor.execute(insert_sql)
        row_count = cursor.rowcount
        conn.commit()
        print(f"Successfully processed {row_count} incremental rows into {target_table}")
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
    dag_id="06_incremental_load_gold_layer",
    description="Incrementally load, enrich, and update Kimball Star Schema into the Gold layer",
    start_date=datetime(2026, 1, 1),
    schedule="0 1 * * *",  # Runs daily at 01:00 UTC (30 min after Silver at 00:30 UTC)
    catchup=False,
    max_active_runs=1,
    max_active_tasks=4,
    default_args=default_args,
    tags=["pulsetrack", "gold", "incremental", "kimball", "star-schema"],
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    # ----------------------------------------------------------------
    # 1. Dimensions (Conformed Dimension Upserts)
    # ----------------------------------------------------------------
    load_dim_geo = PythonOperator(
        task_id="load_gold_dim_geo",
        python_callable=load_gold_incremental,
        op_kwargs={
            "sql_file": "dim_geo.sql",
            "target_table": "pulsetrack_gold.dim_geo",
            "target_columns": ["country", "region", "city", "postal_code"],
        },
    )

    load_dim_device = PythonOperator(
        task_id="load_gold_dim_device",
        python_callable=load_gold_incremental,
        op_kwargs={
            "sql_file": "dim_device.sql",
            "target_table": "pulsetrack_gold.dim_device",
            "target_columns": ["device_type", "operating_system", "browser", "device_model"],
        },
    )

    load_dim_account_manager = PythonOperator(
        task_id="load_gold_dim_account_manager",
        python_callable=load_gold_incremental,
        op_kwargs={
            "sql_file": "dim_account_manager.sql",
            "target_table": "pulsetrack_gold.dim_account_manager",
            "target_columns": ["am_id", "am_name", "created_at", "updated_at", "effective_from", "effective_to", "is_current"],
        },
    )

    load_dim_advertiser = PythonOperator(
        task_id="load_gold_dim_advertiser",
        python_callable=load_gold_incremental,
        op_kwargs={
            "sql_file": "dim_advertiser.sql",
            "target_table": "pulsetrack_gold.dim_advertiser",
            "target_columns": [
                "advertiser_id", "advertiser_name", "account_manager_key",
                "created_at", "updated_at", "effective_from", "effective_to", "is_current",
            ],
        },
    )

    load_dim_partner = PythonOperator(
        task_id="load_gold_dim_partner",
        python_callable=load_gold_incremental,
        op_kwargs={
            "sql_file": "dim_partner.sql",
            "target_table": "pulsetrack_gold.dim_partner",
            "target_columns": [
                "partner_id", "partner_name", "partner_type", "status",
                "created_at", "updated_at", "effective_from", "effective_to", "is_current",
            ],
        },
    )

    load_dim_offer = PythonOperator(
        task_id="load_gold_dim_offer",
        python_callable=load_gold_incremental,
        op_kwargs={
            "sql_file": "dim_offer.sql",
            "target_table": "pulsetrack_gold.dim_offer",
            "target_columns": [
                "offer_id", "offer_name", "advertiser_key", "offer_status",
                "created_at", "updated_at", "effective_from", "effective_to", "is_current",
            ],
        },
    )

    load_dim_campaign = PythonOperator(
        task_id="load_gold_dim_campaign",
        python_callable=load_gold_incremental,
        op_kwargs={
            "sql_file": "dim_campaign.sql",
            "target_table": "pulsetrack_gold.dim_campaign",
            "target_columns": [
                "campaign_id", "campaign_name", "offer_key", "advertiser_key",
                "campaign_status", "created_at", "updated_at", "effective_from",
                "effective_to", "is_current",
            ],
        },
    )

    load_dim_creative = PythonOperator(
        task_id="load_gold_dim_creative",
        python_callable=load_gold_incremental,
        op_kwargs={
            "sql_file": "dim_creative.sql",
            "target_table": "pulsetrack_gold.dim_creative",
            "target_columns": [
                "creative_id", "creative_name", "campaign_key", "creative_type",
                "status", "created_at", "updated_at", "effective_from", "effective_to", "is_current",
            ],
        },
    )

    # ----------------------------------------------------------------
    # 2. Fact Tables (Watermark Appends & Adjustment Upserts)
    # ----------------------------------------------------------------
    load_fact_impression = PythonOperator(
        task_id="load_gold_fact_impression",
        python_callable=load_gold_incremental,
        op_kwargs={
            "sql_file": "fact_impression.sql",
            "target_table": "pulsetrack_gold.fact_impression",
            "target_columns": [
                "impression_id", "date_key", "advertiser_key", "offer_key",
                "campaign_key", "creative_key", "partner_key", "geo_key",
                "device_key", "impression_count",
            ],
            "watermark_table": "pulsetrack_gold.fact_impression",
            "date_col": "date_key",
        },
    )

    load_fact_click = PythonOperator(
        task_id="load_gold_fact_click",
        python_callable=load_gold_incremental,
        op_kwargs={
            "sql_file": "fact_click.sql",
            "target_table": "pulsetrack_gold.fact_click",
            "target_columns": [
                "click_id", "date_key", "advertiser_key", "offer_key",
                "campaign_key", "creative_key", "partner_key", "geo_key",
                "device_key", "click_count",
            ],
            "watermark_table": "pulsetrack_gold.fact_click",
            "date_col": "date_key",
        },
    )

    load_fact_conversion = PythonOperator(
        task_id="load_gold_fact_conversion",
        python_callable=load_gold_incremental,
        op_kwargs={
            "sql_file": "fact_conversion.sql",
            "target_table": "pulsetrack_gold.fact_conversion",
            "target_columns": [
                "conversion_id", "conversion_date_key", "advertiser_key", "offer_key",
                "campaign_key", "partner_key", "account_manager_key", "geo_key",
                "device_key", "conversion_count", "revenue", "payout", "net_revenue", "margin",
            ],
            "watermark_table": "pulsetrack_gold.fact_conversion",
            "date_col": "conversion_date_key",
        },
    )

    # ----------------------------------------------------------------
    # 3. Bridge Table (Multi-Touch Attribution Upserts)
    # ----------------------------------------------------------------
    load_bridge_conversion_touchpoint = PythonOperator(
        task_id="load_gold_bridge_conversion_touchpoint",
        python_callable=load_gold_incremental,
        op_kwargs={
            "sql_file": "bridge_conversion_touchpoint.sql",
            "target_table": "pulsetrack_gold.bridge_conversion_touchpoint",
            "target_columns": [
                "conversion_key", "click_key", "touch_position", "attribution_weight",
            ],
        },
    )

    end = EmptyOperator(
        task_id="end",
        outlets=outlets,
    )

    # ----------------------------------------------------------------
    # Task Dependencies (Kimball Star Schema Hierarchy)
    # ----------------------------------------------------------------

    # Independent dimensions start first
    start >> [
        load_dim_geo,
        load_dim_device,
        load_dim_account_manager,
        load_dim_partner,
    ]

    # Dimension relationship hierarchy:
    # Account Manager -> Advertiser -> Offer -> Campaign -> Creative
    load_dim_account_manager >> load_dim_advertiser >> load_dim_offer >> load_dim_campaign >> load_dim_creative

    all_dimensions = [
        load_dim_geo,
        load_dim_device,
        load_dim_account_manager,
        load_dim_advertiser,
        load_dim_partner,
        load_dim_offer,
        load_dim_campaign,
        load_dim_creative,
    ]

    # Fact tables depend on all updated dimensions
    all_dimensions >> load_fact_impression
    all_dimensions >> load_fact_click
    all_dimensions >> load_fact_conversion

    # Multi-Touch Attribution Bridge table links clicks and conversions
    [load_fact_click, load_fact_conversion] >> load_bridge_conversion_touchpoint

    # Pipeline completion
    [load_fact_impression, load_bridge_conversion_touchpoint] >> end
