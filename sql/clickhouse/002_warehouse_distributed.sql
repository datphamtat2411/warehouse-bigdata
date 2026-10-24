-- Coordinator only; each target exists independently on both shards.
CREATE TABLE IF NOT EXISTS warehouse.sales_daily AS warehouse.sales_daily_local
ENGINE = Distributed('warehouse_cluster', 'warehouse', 'sales_daily_local', cityHash64(item_id, store_id));

CREATE TABLE IF NOT EXISTS warehouse.store_daily_sales AS warehouse.store_daily_sales_local
ENGINE = Distributed('warehouse_cluster', 'warehouse', 'store_daily_sales_local', cityHash64(store_id, sale_date));

CREATE TABLE IF NOT EXISTS warehouse.forecast_daily AS warehouse.forecast_daily_local
ENGINE = Distributed('warehouse_cluster', 'warehouse', 'forecast_daily_local', cityHash64(item_id, store_id));

CREATE TABLE IF NOT EXISTS warehouse.forecast_metrics AS warehouse.forecast_metrics_local
ENGINE = Distributed('warehouse_cluster', 'warehouse', 'forecast_metrics_local', cityHash64(evaluation_run_id, holdout_id, method_id));

CREATE TABLE IF NOT EXISTS warehouse.inventory_scenario AS warehouse.inventory_scenario_local
ENGINE = Distributed('warehouse_cluster', 'warehouse', 'inventory_scenario_local', cityHash64(item_id, store_id));

CREATE TABLE IF NOT EXISTS warehouse.central_supply_scenario AS warehouse.central_supply_scenario_local
ENGINE = Distributed('warehouse_cluster', 'warehouse', 'central_supply_scenario_local', cityHash64(item_id));

CREATE TABLE IF NOT EXISTS warehouse.replenishment AS warehouse.replenishment_local
ENGINE = Distributed('warehouse_cluster', 'warehouse', 'replenishment_local', cityHash64(item_id, store_id));

CREATE TABLE IF NOT EXISTS warehouse.allocation AS warehouse.allocation_local
ENGINE = Distributed('warehouse_cluster', 'warehouse', 'allocation_local', cityHash64(item_id, store_id));

CREATE TABLE IF NOT EXISTS warehouse.policy_evaluation AS warehouse.policy_evaluation_local
ENGINE = Distributed('warehouse_cluster', 'warehouse', 'policy_evaluation_local', cityHash64(item_id, store_id));
