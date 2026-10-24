-- Contract v1. Sort keys index rows; they do not enforce uniqueness.
CREATE DATABASE IF NOT EXISTS warehouse;

CREATE TABLE IF NOT EXISTS warehouse.sales_daily_local
(
    item_id String,
    dept_id String,
    cat_id String,
    store_id String,
    state_id String,
    day_index UInt64,
    sale_date Date,
    unit_sales UInt32,
    source_snapshot_id String
)
ENGINE = MergeTree()
ORDER BY (store_id, item_id, sale_date, source_snapshot_id);

CREATE TABLE IF NOT EXISTS warehouse.store_daily_sales_local
(
    source_snapshot_id String,
    store_id String,
    observed_series_count UInt64,
    sale_date Date,
    total_units UInt64
)
ENGINE = MergeTree()
ORDER BY (store_id, sale_date, source_snapshot_id);

CREATE TABLE IF NOT EXISTS warehouse.forecast_daily_local
(
    forecast_run_id String,
    model_run_id String,
    source_snapshot_id String,
    feature_version String,
    issue_date Date,
    forecast_date Date,
    horizon UInt64,
    item_id String,
    store_id String,
    run_type String,
    created_at DateTime64(3, 'UTC'),
    predicted_units Float64,
    forecast_status LowCardinality(String)
)
ENGINE = MergeTree()
ORDER BY (store_id, item_id, issue_date, forecast_date, forecast_run_id);

CREATE TABLE IF NOT EXISTS warehouse.forecast_metrics_local
(
    evaluation_run_id String,
    model_run_id Nullable(String),
    forecast_run_id Nullable(String),
    holdout_id String,
    method_id String,
    metric_name String,
    horizon UInt64,
    metric_value Nullable(Float64),
    observation_count UInt64,
    sum_absolute_error Float64,
    sum_actual_units Float64,
    evaluation_start Date,
    evaluation_end Date,
    run_type String
)
ENGINE = MergeTree()
ORDER BY (evaluation_run_id, holdout_id, method_id, metric_name, horizon);

CREATE TABLE IF NOT EXISTS warehouse.inventory_scenario_local
(
    scenario_id String,
    scenario_version String,
    scenario_type String,
    source_snapshot_id String,
    issue_date Date,
    item_id String,
    store_id String,
    opening_stock_units UInt64,
    safety_stock_units UInt64,
    parameter_digest String,
    origin_label String,
    scenario_seed Nullable(Int32)
)
ENGINE = MergeTree()
ORDER BY (store_id, item_id, issue_date, scenario_id);

CREATE TABLE IF NOT EXISTS warehouse.central_supply_scenario_local
(
    scenario_id String,
    issue_date Date,
    item_id String,
    source_warehouse_id String,
    central_available_units UInt64,
    origin_label String
)
ENGINE = MergeTree()
ORDER BY (item_id, issue_date, scenario_id);

CREATE TABLE IF NOT EXISTS warehouse.replenishment_local
(
    decision_run_id String,
    source_snapshot_id String,
    forecast_run_id String,
    model_run_id String,
    scenario_id String,
    policy_version String,
    issue_date Date,
    item_id String,
    store_id String,
    forecast_7d_units Float64,
    planning_demand_units UInt64,
    opening_stock_units UInt64,
    safety_stock_units UInt64,
    target_stock_units UInt64,
    critical_need_units UInt64,
    safety_need_units UInt64,
    request_units UInt64,
    forecast_shortage_flag UInt8,
    origin_label String
)
ENGINE = MergeTree()
ORDER BY (store_id, item_id, issue_date, decision_run_id);

CREATE TABLE IF NOT EXISTS warehouse.allocation_local
(
    decision_run_id String,
    source_snapshot_id String,
    forecast_run_id String,
    model_run_id String,
    scenario_id String,
    policy_id String,
    policy_version String,
    issue_date Date,
    item_id String,
    store_id String,
    source_warehouse_id String,
    requested_units UInt64,
    critical_allocated_units Nullable(UInt64),
    safety_allocated_units Nullable(UInt64),
    allocated_units UInt64,
    unmet_request_units UInt64,
    central_available_units UInt64
)
ENGINE = MergeTree()
ORDER BY (store_id, item_id, issue_date, decision_run_id, policy_id);

CREATE TABLE IF NOT EXISTS warehouse.policy_evaluation_local
(
    evaluation_run_id String,
    source_snapshot_id String,
    forecast_run_id String,
    scenario_id String,
    decision_run_id String,
    policy_id String,
    policy_version String,
    issue_date Date,
    item_id String,
    store_id String,
    actual_sales_proxy_7d_units UInt64,
    simulated_fulfilled_units UInt64,
    simulated_unmet_units UInt64,
    ending_stock_units UInt64,
    excess_over_safety_units UInt64,
    simulated_stockout_days UInt64,
    run_type String,
    observed_label_window_start Date,
    observed_label_window_end Date
)
ENGINE = MergeTree()
ORDER BY (store_id, item_id, issue_date, evaluation_run_id, policy_id);
