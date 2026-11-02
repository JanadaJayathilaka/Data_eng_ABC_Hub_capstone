"""
PulseTrack: Data Quality & End-to-End Reconciliation DAG (07_data_quality_and_reconciliation.py)
================================================================================================
Orchestrates automated governance, data quality testing, and layer reconciliation:
1. Audits pipeline execution in pulsetrack_audit.pipeline_runs.
2. Reconciles records across Source -> Bronze -> Silver -> Gold (reconciliation_summary).
3. Executes automated quality tests across Schema, Nullability, Duplicates, Referential
   Integrity, Value Ranges, Business Rules, and Multi-Touch Attribution.
4. Generates an executive audit scorecard in the Airflow task log.
"""

import uuid
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
try:
    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.empty import EmptyOperator
    from airflow.operators.python import PythonOperator

from utils.database import get_connection
from utils.file import read_sql

BASE_DIR = Path(__file__).resolve().parent
SQL_DIR = BASE_DIR / "sql" / "audit"
TARGET_CONN = "postgres_dw"


def start_audit_run(**context):
    """
    Initializes an audit run entry in pulsetrack_audit.pipeline_runs.
    """
    dag_run = context.get("dag_run")
    run_id = dag_run.run_id if dag_run else f"manual_dq_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    batch_id = str(uuid.uuid4())[:8]

    conn = get_connection(TARGET_CONN)
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            INSERT INTO pulsetrack_audit.pipeline_runs (
                run_id, batch_id, pipeline_id, layer, start_time, status
            )
            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, 'RUNNING')
            ON CONFLICT (run_id) DO UPDATE SET
                start_time = CURRENT_TIMESTAMP,
                status = 'RUNNING';
            """,
            (run_id, batch_id, "07_data_quality_and_reconciliation", "E2E"),
        )
        conn.commit()
        print(f"Audit run initialized with run_id: {run_id}, batch_id: {batch_id}")
    finally:
        cursor.close()
        conn.close()

    context["ti"].xcom_push(key="audit_run_id", value=run_id)
    return run_id


def run_reconciliation(**context):
    """
    Executes end-to-end reconciliation across Source -> Bronze -> Silver -> Gold.
    """
    ti = context["ti"]
    run_id = ti.xcom_pull(task_ids="start_audit_run", key="audit_run_id")

    sql_template = read_sql(SQL_DIR / "reconciliation.sql")
    sql = sql_template.replace("{{ run_id }}", run_id)

    conn = get_connection(TARGET_CONN)
    cursor = conn.cursor()

    try:
        cursor.execute(sql)
        conn.commit()

        # Fetch results for logging
        cursor.execute(
            """
            SELECT entity_name, source_count, bronze_count, silver_valid_count,
                   silver_rejected_count, silver_duplicate_count, gold_count,
                   variance_count, reconciliation_status
            FROM pulsetrack_audit.layer_reconciliation
            WHERE run_id = %s
            ORDER BY entity_name;
            """,
            (run_id,),
        )
        rows = cursor.fetchall()

        print("\n" + "=" * 105)
        print("LAYER RECONCILIATION REPORT (Source -> Bronze -> Silver -> Gold)")
        print("=" * 105)
        fmt = "{:<24} | {:<10} | {:<10} | {:<12} | {:<12} | {:<10} | {:<12}"
        print(fmt.format("Entity", "Source", "Bronze", "Silver Valid", "Rejected/Dup", "Gold", "Status"))
        print("-" * 105)
        for r in rows:
            rej_dup = f"{r[4]}/{r[5]}"
            print(fmt.format(r[0], r[1], r[2], r[3], rej_dup, r[6], r[8]))
        print("=" * 105 + "\n")

    finally:
        cursor.close()
        conn.close()


def run_quality_checks(**context):
    """
    Executes all automated quality tests across the 6 required categories.
    """
    ti = context["ti"]
    run_id = ti.xcom_pull(task_ids="start_audit_run", key="audit_run_id")

    sql_template = read_sql(SQL_DIR / "quality_checks.sql")
    sql = sql_template.replace("{{ run_id }}", run_id)

    conn = get_connection(TARGET_CONN)
    cursor = conn.cursor()

    try:
        cursor.execute(sql)
        conn.commit()

        cursor.execute(
            """
            SELECT category, check_name, target_table, records_evaluated, failed_records, status
            FROM pulsetrack_audit.data_quality_results
            WHERE run_id = %s
            ORDER BY category, check_name;
            """,
            (run_id,),
        )
        checks = cursor.fetchall()

        passed_cnt = sum(1 for c in checks if c[5] == "PASSED")
        warning_cnt = sum(1 for c in checks if c[5] == "WARNING")
        failed_cnt = sum(1 for c in checks if c[5] == "FAILED")

        print("\n" + "=" * 115)
        print("DATA QUALITY SCORECARD")
        print("=" * 115)
        fmt = "{:<22} | {:<38} | {:<12} | {:<12} | {:<10}"
        print(fmt.format("Category", "Check Name", "Evaluated", "Failed Rows", "Status"))
        print("-" * 115)
        for c in checks:
            print(fmt.format(c[0], c[1], c[3], c[4], c[5]))
        print("=" * 115)
        print(f"Summary: {passed_cnt} PASSED | {warning_cnt} WARNINGS | {failed_cnt} FAILED\n")

        if failed_cnt > 0:
            raise ValueError(f"Data quality testing failed: {failed_cnt} critical test(s) failed!")

    finally:
        cursor.close()
        conn.close()


def finalize_audit_run(**context):
    """
    Aggregates metrics and finalizes the audit entry in pulsetrack_audit.pipeline_runs.
    """
    ti = context["ti"]
    run_id = ti.xcom_pull(task_ids="start_audit_run", key="audit_run_id")

    conn = get_connection(TARGET_CONN)
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            WITH recon_agg AS (
                SELECT 
                    COALESCE(SUM(source_count), 0) AS total_source,
                    COALESCE(SUM(silver_valid_count), 0) AS total_processed,
                    COALESCE(SUM(gold_count), 0) AS total_inserted,
                    COALESCE(SUM(silver_rejected_count), 0) AS total_rejected
                FROM pulsetrack_audit.layer_reconciliation
                WHERE run_id = %s
            ),
            dq_agg AS (
                SELECT 
                    COUNT(*) FILTER (WHERE status = 'FAILED') AS total_failures,
                    COUNT(*) FILTER (WHERE status = 'WARNING') AS total_warnings
                FROM pulsetrack_audit.data_quality_results
                WHERE run_id = %s
            )
            UPDATE pulsetrack_audit.pipeline_runs
            SET 
                end_time = CURRENT_TIMESTAMP,
                source_record_count = (SELECT total_source FROM recon_agg),
                processed_count = (SELECT total_processed FROM recon_agg),
                inserted_count = (SELECT total_inserted FROM recon_agg),
                rejected_count = (SELECT total_rejected FROM recon_agg),
                error_count = (SELECT total_failures FROM dq_agg),
                status = CASE 
                    WHEN (SELECT total_failures FROM dq_agg) > 0 THEN 'FAILED'
                    WHEN (SELECT total_warnings FROM dq_agg) > 0 THEN 'WARNING'
                    ELSE 'SUCCESS'
                END
            WHERE run_id = %s;
            """,
            (run_id, run_id, run_id),
        )
        conn.commit()
        print(f"Audit run {run_id} successfully finalized.")
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
    dag_id="07_data_quality_and_reconciliation",
    description="Automated Data Quality Testing, Layer Reconciliation, and Warehouse Governance Audit",
    start_date=datetime(2026, 1, 1),
    schedule="30 1 * * *",  # Runs daily at 01:30 UTC (30 min after Gold layer at 01:00 UTC)
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["pulsetrack", "governance", "reconciliation", "quality", "audit"],
) as dag:

    start = EmptyOperator(task_id="start")

    audit_start = PythonOperator(
        task_id="start_audit_run",
        python_callable=start_audit_run,
    )

    reconciliation = PythonOperator(
        task_id="execute_layer_reconciliation",
        python_callable=run_reconciliation,
    )

    quality_checks = PythonOperator(
        task_id="execute_data_quality_checks",
        python_callable=run_quality_checks,
    )

    audit_finalize = PythonOperator(
        task_id="finalize_audit_run",
        python_callable=finalize_audit_run,
    )

    end = EmptyOperator(task_id="end")

    # Workflow Dependencies
    start >> audit_start >> [reconciliation, quality_checks] >> audit_finalize >> end
