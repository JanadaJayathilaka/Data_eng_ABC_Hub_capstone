-- ============================================================================
-- PulseTrack: Audit, Quality & Reconciliation Schema
-- ============================================================================
-- Establishes the governance and trust framework for the warehouse:
-- 1. pipeline_runs: High-level operational audit log for batch execution
-- 2. layer_reconciliation: Source -> Bronze -> Silver -> Gold row reconciliation
-- 3. data_quality_results: Automated quality test results across 6 categories
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS pulsetrack_audit;

-- 1. Pipeline Runs (Batch & Execution Auditing)
CREATE TABLE IF NOT EXISTS pulsetrack_audit.pipeline_runs (
    run_id              VARCHAR(128) PRIMARY KEY,
    batch_id            VARCHAR(64),
    pipeline_id         VARCHAR(64) NOT NULL,
    layer               VARCHAR(20) NOT NULL,  -- BRONZE, SILVER, GOLD, E2E
    start_time          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    end_time            TIMESTAMP,
    status              VARCHAR(20) NOT NULL DEFAULT 'RUNNING',  -- RUNNING, SUCCESS, FAILED, WARNING
    source_record_count BIGINT DEFAULT 0,
    processed_count     BIGINT DEFAULT 0,
    inserted_count      BIGINT DEFAULT 0,
    updated_count       BIGINT DEFAULT 0,
    rejected_count      BIGINT DEFAULT 0,
    error_count         BIGINT DEFAULT 0,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Layer Reconciliation Table (Source -> Bronze -> Silver -> Gold)
CREATE TABLE IF NOT EXISTS pulsetrack_audit.layer_reconciliation (
    reconciliation_id       SERIAL PRIMARY KEY,
    run_id                  VARCHAR(128) REFERENCES pulsetrack_audit.pipeline_runs(run_id) ON DELETE CASCADE,
    reconciliation_time     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    entity_name             VARCHAR(100) NOT NULL,
    source_count            BIGINT DEFAULT 0,
    bronze_count            BIGINT DEFAULT 0,
    silver_valid_count      BIGINT DEFAULT 0,
    silver_rejected_count   BIGINT DEFAULT 0,
    silver_duplicate_count  BIGINT DEFAULT 0,
    gold_count              BIGINT DEFAULT 0,
    variance_count          BIGINT DEFAULT 0,
    reconciliation_status   VARCHAR(30) NOT NULL,  -- RECONCILED, EXPLAINED_DISCREPANCY, MISMATCH
    notes                   TEXT
);

-- 3. Data Quality Results Table
CREATE TABLE IF NOT EXISTS pulsetrack_audit.data_quality_results (
    check_id            SERIAL PRIMARY KEY,
    run_id              VARCHAR(128) REFERENCES pulsetrack_audit.pipeline_runs(run_id) ON DELETE CASCADE,
    checked_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    category            VARCHAR(50) NOT NULL,  -- SCHEMA, NULLABILITY, DUPLICATES, REFERENTIAL_INTEGRITY, RANGE_VALIDATION, BUSINESS_RULES, ATTRIBUTION
    check_name          VARCHAR(150) NOT NULL,
    target_layer        VARCHAR(20) NOT NULL,  -- SILVER, GOLD
    target_table        VARCHAR(100) NOT NULL,
    records_evaluated   BIGINT DEFAULT 0,
    failed_records      BIGINT DEFAULT 0,
    status              VARCHAR(20) NOT NULL,  -- PASSED, WARNING, FAILED
    details             TEXT
);

-- Indexes for audit query performance
CREATE INDEX IF NOT EXISTS idx_reconciliation_run_id ON pulsetrack_audit.layer_reconciliation(run_id);
CREATE INDEX IF NOT EXISTS idx_dq_results_run_id ON pulsetrack_audit.data_quality_results(run_id);
CREATE INDEX IF NOT EXISTS idx_dq_results_status ON pulsetrack_audit.data_quality_results(status);
