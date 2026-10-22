"""Immutable logical dataset and storage-boundary contracts."""

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Final

STRING: Final = "String"
DATE: Final = "Date"
TIMESTAMP_UTC: Final = "TimestampUTC"
NON_NEGATIVE_INT: Final = "NonNegativeInt"
INTEGER: Final = "Integer"
FINITE_FLOAT: Final = "FiniteFloat"
NON_NEGATIVE_FLOAT: Final = "NonNegativeFloat"
BOOL: Final = "Bool"

LOGICAL_TYPES: Final = frozenset(
    {
        STRING,
        DATE,
        TIMESTAMP_UTC,
        NON_NEGATIVE_INT,
        INTEGER,
        FINITE_FLOAT,
        NON_NEGATIVE_FLOAT,
        BOOL,
    }
)


@dataclass(frozen=True, slots=True)
class FieldSpec:
    """One logical field in a dataset or artifact contract."""

    name: str
    logical_type: str
    nullable: bool = False
    description: str = ""


@dataclass(frozen=True, slots=True)
class LineageAuthority:
    """Where a required lineage value is authoritative."""

    name: str
    location: str


@dataclass(frozen=True, slots=True)
class DatasetContract:
    """Logical identity, schema, grain, lineage, and invariant metadata."""

    name: str
    version: str
    producer: str
    consumer: str
    grain: str
    unique_key: tuple[str, ...]
    fields: tuple[FieldSpec, ...]
    provenance: str
    required_lineage: tuple[str, ...]
    lineage_authority: tuple[LineageAuthority, ...]
    invariants: tuple[str, ...]
    serving_name: str | None = None

    @property
    def field_names(self) -> tuple[str, ...]:
        """Return fields in the declared logical order."""

        return tuple(field.name for field in self.fields)

    @property
    def required_field_names(self) -> tuple[str, ...]:
        """Return non-nullable logical fields."""

        return tuple(field.name for field in self.fields if not field.nullable)


@dataclass(frozen=True, slots=True)
class FieldMapping:
    """Mapping for a logical field crossing the Parquet/ClickHouse boundary."""

    dataset_name: str
    logical_field: str
    parquet_field: str
    serving_field: str | None
    logical_type: str
    serving_type: str | None
    nullable: bool
    conversion: str
    retention_location: str
    query_aliases: tuple[str, ...] = ()
    approval_required: bool = False


@dataclass(frozen=True, slots=True)
class ServingDatasetMapping:
    """Dataset-level serving name and omitted-field metadata."""

    logical_dataset: str
    serving_dataset: str
    logical_key: tuple[str, ...]
    field_mappings: tuple[FieldMapping, ...]
    lineage_fields: tuple[str, ...]
    omitted_fields: tuple[str, ...]
    metadata_location: str
    extra_serving_fields: tuple[str, ...] = ()


def _field(
    name: str,
    logical_type: str,
    *,
    nullable: bool = False,
    description: str = "",
) -> FieldSpec:
    return FieldSpec(
        name=name,
        logical_type=logical_type,
        nullable=nullable,
        description=description,
    )


_SALES_DAILY_FIELDS: Final = (
    _field("item_id", STRING, description="M5 item identifier."),
    _field("dept_id", STRING),
    _field("cat_id", STRING),
    _field("store_id", STRING),
    _field("state_id", STRING),
    _field("series_id", STRING, description="Source series audit identifier."),
    _field("day_key", STRING, description="M5 relative day key."),
    _field("day_index", NON_NEGATIVE_INT),
    _field("date", DATE),
    _field("wm_yr_wk", NON_NEGATIVE_INT),
    _field("unit_sales", NON_NEGATIVE_INT, description="Observed M5 units sold."),
)


SALES_DAILY = DatasetContract(
    name="sales_daily",
    version="1.0",
    producer="J02 normalization",
    consumer="J03 enrichment, J04 features, J08 publication",
    grain="One observed item-store-date within one source snapshot.",
    unique_key=("item_id", "store_id", "date"),
    fields=_SALES_DAILY_FIELDS,
    provenance="observed_m5_unit_sales",
    required_lineage=("source_snapshot_id",),
    lineage_authority=(LineageAuthority("source_snapshot_id", "manifest_or_path"),),
    invariants=(
        "unit_sales is observed recorded sales, not verified demand or inventory.",
        "Zero-sales observations are retained.",
        "The source snapshot is part of the physical uniqueness scope.",
        "Calendar mapping is one-to-one and every row has a valid date.",
    ),
    serving_name="sales_daily",
)


SALES_ENRICHED = DatasetContract(
    name="sales_enriched",
    version="1.0",
    producer="J03 enrichment and aggregation",
    consumer="J04 features, analytics consumers, J08 publication",
    grain="One observed item-store-date within one source snapshot.",
    unique_key=("item_id", "store_id", "date"),
    fields=_SALES_DAILY_FIELDS
    + (
        _field("sell_price", FINITE_FLOAT, nullable=True),
        _field("price_missing", BOOL),
        _field("wday", INTEGER),
        _field("month", INTEGER),
        _field("year", INTEGER),
        _field("snap_applicable", BOOL),
        _field("is_zero_sale", BOOL),
        _field("event_name_1", STRING, nullable=True),
        _field("event_type_1", STRING, nullable=True),
        _field("event_name_2", STRING, nullable=True),
        _field("event_type_2", STRING, nullable=True),
    ),
    provenance="derived_from_observed_m5_unit_sales",
    required_lineage=("source_snapshot_id",),
    lineage_authority=(LineageAuthority("source_snapshot_id", "manifest_or_path"),),
    invariants=(
        "Price enrichment preserves sales fact cardinality.",
        "Missing price remains nullable and is flagged; it is not replaced with zero.",
        "is_zero_sale describes observed zero sales and is not a missingness flag.",
    ),
)


STORE_DAILY_SALES = DatasetContract(
    name="store_daily_sales",
    version="1.0",
    producer="J03 aggregation",
    consumer="J08 publication and Streamlit overview",
    grain="One source snapshot-store-date aggregate.",
    unique_key=("source_snapshot_id", "store_id", "date"),
    fields=(
        _field("source_snapshot_id", STRING),
        _field("store_id", STRING),
        _field("date", DATE),
        _field("total_units", NON_NEGATIVE_INT),
        _field("observed_series_count", NON_NEGATIVE_INT),
    ),
    provenance="derived_from_observed_m5_unit_sales",
    required_lineage=("source_snapshot_id",),
    lineage_authority=(LineageAuthority("source_snapshot_id", "row"),),
    invariants=(
        "total_units equals the scoped sum of sales_daily.unit_sales.",
        "total_units is not a transaction or customer-order count.",
    ),
    serving_name="store_daily_sales",
)


CATEGORY_MONTHLY_SALES = DatasetContract(
    name="category_monthly_sales",
    version="1.0",
    producer="J03 aggregation",
    consumer="J08 publication and category dashboard",
    grain="One source snapshot-category-month aggregate.",
    unique_key=("source_snapshot_id", "cat_id", "month_start"),
    fields=(
        _field("source_snapshot_id", STRING),
        _field("cat_id", STRING),
        _field("month_start", DATE),
        _field("total_units", NON_NEGATIVE_INT),
    ),
    provenance="derived_from_observed_m5_unit_sales",
    required_lineage=("source_snapshot_id",),
    lineage_authority=(LineageAuthority("source_snapshot_id", "row"),),
    invariants=(
        "total_units equals the scoped sum of sales_daily.unit_sales.",
        "total_units is a nonnegative Long aggregate, not a transaction or order count.",
    ),
)


FORECAST_FEATURES = DatasetContract(
    name="forecast_features",
    version="1.0",
    producer="J04 feature engineering",
    consumer="J05 training and J06 scoring",
    grain="One item-store-target-date feature row scoped by source, feature version, and role.",
    unique_key=("item_id", "store_id", "target_date"),
    fields=(
        _field("source_snapshot_id", STRING),
        _field("feature_version", STRING),
        _field("feature_role", STRING),
        _field("item_id", STRING),
        _field("store_id", STRING),
        _field("dept_id", STRING),
        _field("target_date", DATE),
        _field("as_of_date", DATE),
        _field("lag_1", NON_NEGATIVE_INT),
        _field("lag_7", NON_NEGATIVE_INT),
        _field("lag_28", NON_NEGATIVE_INT),
        _field("rolling_mean_7", NON_NEGATIVE_FLOAT),
        _field("rolling_mean_28", NON_NEGATIVE_FLOAT),
        _field("day_of_week", NON_NEGATIVE_INT),
        _field("month", NON_NEGATIVE_INT),
        _field("is_weekend", BOOL),
        _field("event_flag", BOOL),
        _field("price_missing", BOOL),
        _field("last_known_price", FINITE_FLOAT, nullable=True),
        _field("label", NON_NEGATIVE_INT, nullable=True),
    ),
    provenance="derived_leakage_controlled_features",
    required_lineage=("source_snapshot_id", "feature_version"),
    lineage_authority=(
        LineageAuthority("source_snapshot_id", "row"),
        LineageAuthority("feature_version", "row"),
    ),
    invariants=(
        "Feature values use only information available by as_of_date.",
        "label is present only for historical training/evaluation roles.",
        "Forward scoring does not contain future labels or future prices.",
    ),
)


FORECAST_DAILY = DatasetContract(
    name="forecast_daily",
    version="1.0",
    producer="J06 forecast inference",
    consumer="J07 decisions, J08 publication, Streamlit forecast reads",
    grain="One forecast-run item-store-forecast-date row.",
    unique_key=("forecast_run_id", "item_id", "store_id", "forecast_date"),
    fields=(
        _field("forecast_run_id", STRING),
        _field("model_run_id", STRING),
        _field("source_snapshot_id", STRING),
        _field("feature_version", STRING),
        _field("issue_date", DATE),
        _field("forecast_date", DATE),
        _field("horizon", NON_NEGATIVE_INT),
        _field("item_id", STRING),
        _field("store_id", STRING),
        _field("raw_prediction", FINITE_FLOAT),
        _field("predicted_units", NON_NEGATIVE_FLOAT),
        _field("run_type", STRING),
        _field("created_at", TIMESTAMP_UTC),
    ),
    provenance="predicted_unit_sales",
    required_lineage=(
        "forecast_run_id",
        "model_run_id",
        "source_snapshot_id",
        "feature_version",
    ),
    lineage_authority=tuple(
        LineageAuthority(name, "row")
        for name in (
            "forecast_run_id",
            "model_run_id",
            "source_snapshot_id",
            "feature_version",
        )
    ),
    invariants=(
        "predicted_units is predicted expected unit sales, not observed sales or verified demand.",
        "Each completed scored series has exactly seven horizons from 1 through 7.",
        "forecast_date equals issue_date plus horizon calendar days.",
        "Daily predictions remain finite and unrounded until decision aggregation.",
        "Chronological evaluation prevents future-label leakage.",
    ),
    serving_name="forecast_daily",
)


FORECAST_METRICS = DatasetContract(
    name="forecast_metrics",
    version="1.0",
    producer="J05/J06 forecast evaluation",
    consumer="J08 publication and Streamlit forecast evaluation reads",
    grain="One evaluation-run holdout-method-metric-horizon row.",
    unique_key=("evaluation_run_id", "holdout_id", "method_id", "metric_name", "horizon"),
    fields=(
        _field("evaluation_run_id", STRING),
        _field("model_run_id", STRING, nullable=True),
        _field("forecast_run_id", STRING, nullable=True),
        _field("holdout_id", STRING),
        _field("method_id", STRING),
        _field("metric_name", STRING),
        _field("horizon", NON_NEGATIVE_INT),
        _field("metric_value", FINITE_FLOAT, nullable=True),
        _field("observation_count", NON_NEGATIVE_INT),
        _field("sum_absolute_error", NON_NEGATIVE_FLOAT),
        _field("sum_actual_units", NON_NEGATIVE_FLOAT),
        _field("evaluation_start", DATE),
        _field("evaluation_end", DATE),
        _field("run_type", STRING),
    ),
    provenance="forecast_quality_evaluation",
    required_lineage=("evaluation_run_id", "holdout_id"),
    lineage_authority=(
        LineageAuthority("evaluation_run_id", "row"),
        LineageAuthority("holdout_id", "row"),
    ),
    invariants=(
        "Forecast metrics compare methods on the same observed-label cohort.",
        "metric_value is nullable when a metric such as WAPE is undefined.",
        "Forecast quality is distinct from inventory-policy evaluation.",
        "horizon zero is pooled only when explicitly documented; horizons 1 through 7 are daily.",
    ),
    serving_name="forecast_metrics",
)


INVENTORY_SCENARIO = DatasetContract(
    name="inventory_scenario",
    version="1.0",
    producer="J07 scenario generation",
    consumer="J07 decisions, J08 publication, Streamlit inventory reads",
    grain="One immutable scenario-issue-date-item-store input row.",
    unique_key=("scenario_id", "issue_date", "item_id", "store_id"),
    fields=(
        _field("scenario_id", STRING),
        _field("scenario_version", STRING),
        _field("scenario_type", STRING),
        _field("source_snapshot_id", STRING),
        _field("issue_date", DATE),
        _field("item_id", STRING),
        _field("store_id", STRING),
        _field("opening_stock_units", NON_NEGATIVE_INT),
        _field("safety_stock_units", NON_NEGATIVE_INT),
        _field("parameter_digest", STRING),
        _field("origin_label", STRING),
        _field("scenario_seed", INTEGER, nullable=True),
    ),
    provenance="simulated_inventory_inputs",
    required_lineage=("scenario_id", "source_snapshot_id"),
    lineage_authority=(
        LineageAuthority("scenario_id", "row"),
        LineageAuthority("source_snapshot_id", "row"),
    ),
    invariants=(
        "All stock quantities are simulated nonnegative integers.",
        "Scenario inputs are immutable; changing inputs creates a new scenario identity.",
        "origin_label explicitly identifies simulated provenance.",
    ),
    serving_name="inventory_scenario",
)


CENTRAL_SUPPLY_SCENARIO = DatasetContract(
    name="central_supply_scenario",
    version="1.0",
    producer="J07 scenario generation",
    consumer="J07 allocation, J08 publication, Streamlit distribution reads",
    grain="One immutable scenario-issue-date-item central supply row.",
    unique_key=("scenario_id", "issue_date", "item_id"),
    fields=(
        _field("scenario_id", STRING),
        _field("issue_date", DATE),
        _field("item_id", STRING),
        _field("source_warehouse_id", STRING),
        _field("central_available_units", NON_NEGATIVE_INT),
        _field("origin_label", STRING),
    ),
    provenance="simulated_central_supply",
    required_lineage=("scenario_id",),
    lineage_authority=(LineageAuthority("scenario_id", "row"),),
    invariants=(
        "Central supply is one simulated product-level capacity, not one capacity per store.",
        "Repeated display of capacity on allocation rows must not be summed as independent supply.",
    ),
    serving_name="central_supply_scenario",
)


REPLENISHMENT_RECOMMENDATIONS = DatasetContract(
    name="replenishment_recommendations",
    version="1.0",
    producer="J07 replenishment policy",
    consumer="J08 publication and Streamlit inventory reads",
    grain="One decision-run item-store recommendation row.",
    unique_key=("decision_run_id", "item_id", "store_id"),
    fields=(
        _field("decision_run_id", STRING),
        _field("source_snapshot_id", STRING),
        _field("forecast_run_id", STRING),
        _field("model_run_id", STRING),
        _field("scenario_id", STRING),
        _field("policy_version", STRING),
        _field("issue_date", DATE),
        _field("item_id", STRING),
        _field("store_id", STRING),
        _field("forecast_7d_units", NON_NEGATIVE_FLOAT),
        _field("planning_demand_units", NON_NEGATIVE_INT),
        _field("opening_stock_units", NON_NEGATIVE_INT),
        _field("safety_stock_units", NON_NEGATIVE_INT),
        _field("target_stock_units", NON_NEGATIVE_INT),
        _field("critical_need_units", NON_NEGATIVE_INT),
        _field("safety_need_units", NON_NEGATIVE_INT),
        _field("request_units", NON_NEGATIVE_INT),
        _field("forecast_shortage_flag", BOOL),
        _field("origin_label", STRING),
    ),
    provenance="calculated_simulated_replenishment_recommendation",
    required_lineage=(
        "decision_run_id",
        "source_snapshot_id",
        "forecast_run_id",
        "model_run_id",
        "scenario_id",
        "policy_version",
    ),
    lineage_authority=tuple(
        LineageAuthority(name, "row")
        for name in (
            "decision_run_id",
            "source_snapshot_id",
            "forecast_run_id",
            "model_run_id",
            "scenario_id",
            "policy_version",
        )
    ),
    invariants=(
        "Recommendations use simulated inventory inputs and predicted unit sales.",
        "Seven unrounded forecasts are aggregated before one documented integer ceiling.",
        "request_units is a recommendation, not an executed purchase order.",
    ),
    serving_name="replenishment",
)


ALLOCATION_RECOMMENDATIONS = DatasetContract(
    name="allocation_recommendations",
    version="1.0",
    producer="J07 allocation policy",
    consumer="J08 publication and Streamlit distribution reads",
    grain="One decision-run policy item-store allocation row.",
    unique_key=("decision_run_id", "policy_id", "item_id", "store_id"),
    fields=(
        _field("decision_run_id", STRING),
        _field("source_snapshot_id", STRING),
        _field("forecast_run_id", STRING),
        _field("model_run_id", STRING),
        _field("scenario_id", STRING),
        _field("policy_id", STRING),
        _field("policy_version", STRING),
        _field("issue_date", DATE),
        _field("item_id", STRING),
        _field("store_id", STRING),
        _field("source_warehouse_id", STRING),
        _field("requested_units", NON_NEGATIVE_INT),
        _field("critical_allocated_units", NON_NEGATIVE_INT, nullable=True),
        _field("safety_allocated_units", NON_NEGATIVE_INT, nullable=True),
        _field("allocated_units", NON_NEGATIVE_INT),
        _field("unmet_request_units", NON_NEGATIVE_INT),
        _field("central_available_units", NON_NEGATIVE_INT),
    ),
    provenance="calculated_simulated_allocation_recommendation",
    required_lineage=(
        "decision_run_id",
        "source_snapshot_id",
        "forecast_run_id",
        "model_run_id",
        "scenario_id",
        "policy_id",
        "policy_version",
    ),
    lineage_authority=tuple(
        LineageAuthority(name, "row")
        for name in (
            "decision_run_id",
            "source_snapshot_id",
            "forecast_run_id",
            "model_run_id",
            "scenario_id",
            "policy_id",
            "policy_version",
        )
    ),
    invariants=(
        "Each store allocation is nonnegative and does not exceed its request.",
        "Product allocation does not exceed one central simulated supply row.",
        "Central capacity repeated for display is not independently summed per store.",
    ),
    serving_name="allocation",
)


INVENTORY_POLICY_EVALUATION = DatasetContract(
    name="inventory_policy_evaluation",
    version="1.0",
    producer="J07 historical policy evaluation",
    consumer="J08 publication and Streamlit historical comparison reads",
    grain="One evaluation-run decision-policy item-store-issue-date outcome row.",
    unique_key=(
        "evaluation_run_id",
        "decision_run_id",
        "policy_id",
        "item_id",
        "store_id",
        "issue_date",
    ),
    fields=(
        _field("evaluation_run_id", STRING),
        _field("source_snapshot_id", STRING),
        _field("forecast_run_id", STRING),
        _field("scenario_id", STRING),
        _field("decision_run_id", STRING),
        _field("policy_id", STRING),
        _field("policy_version", STRING),
        _field("issue_date", DATE),
        _field("item_id", STRING),
        _field("store_id", STRING),
        _field("actual_sales_proxy_7d_units", NON_NEGATIVE_INT),
        _field("simulated_fulfilled_units", NON_NEGATIVE_INT),
        _field("simulated_unmet_units", NON_NEGATIVE_INT),
        _field("ending_stock_units", NON_NEGATIVE_INT),
        _field("excess_over_safety_units", NON_NEGATIVE_INT),
        _field("simulated_stockout_days", NON_NEGATIVE_INT),
        _field("run_type", STRING),
        _field("observed_label_window_start", DATE),
        _field("observed_label_window_end", DATE),
    ),
    provenance="evaluated_simulated_inventory_policy",
    required_lineage=(
        "evaluation_run_id",
        "source_snapshot_id",
        "forecast_run_id",
        "scenario_id",
        "decision_run_id",
        "policy_id",
        "policy_version",
    ),
    lineage_authority=tuple(
        LineageAuthority(name, "row")
        for name in (
            "evaluation_run_id",
            "source_snapshot_id",
            "forecast_run_id",
            "scenario_id",
            "decision_run_id",
            "policy_id",
            "policy_version",
        )
    ),
    invariants=(
        "Outcome metrics use historical observed sales only as a declared demand proxy.",
        "Forward-demo outputs cannot contain fabricated outcome metrics.",
        "Forecast-quality and policy-quality evaluations remain separate artifacts.",
    ),
    serving_name="policy_evaluation",
)


DATASET_CONTRACTS: Final = (
    SALES_DAILY,
    SALES_ENRICHED,
    STORE_DAILY_SALES,
    CATEGORY_MONTHLY_SALES,
    FORECAST_FEATURES,
    FORECAST_DAILY,
    FORECAST_METRICS,
    INVENTORY_SCENARIO,
    CENTRAL_SUPPLY_SCENARIO,
    REPLENISHMENT_RECOMMENDATIONS,
    ALLOCATION_RECOMMENDATIONS,
    INVENTORY_POLICY_EVALUATION,
)

DATASET_CONTRACTS_BY_NAME: Mapping[str, DatasetContract] = MappingProxyType(
    {contract.name: contract for contract in DATASET_CONTRACTS}
)


def _mapping(
    dataset_name: str,
    logical_field: str,
    *,
    serving_field: str | None,
    serving_type: str | None,
    conversion: str,
    retention_location: str = "Parquet and ClickHouse",
    query_aliases: tuple[str, ...] = (),
    approval_required: bool = False,
) -> FieldMapping:
    field = next(
        field
        for field in DATASET_CONTRACTS_BY_NAME[dataset_name].fields
        if field.name == logical_field
    )
    return FieldMapping(
        dataset_name=dataset_name,
        logical_field=logical_field,
        parquet_field=logical_field,
        serving_field=serving_field,
        logical_type=field.logical_type,
        serving_type=serving_type,
        nullable=field.nullable,
        conversion=conversion,
        retention_location=retention_location,
        query_aliases=query_aliases,
        approval_required=approval_required,
    )


_SERVING_TYPES: Mapping[str, str] = MappingProxyType(
    {
        STRING: "String",
        DATE: "Date",
        TIMESTAMP_UTC: "DateTime64(3, 'UTC')",
        NON_NEGATIVE_INT: "UInt64",
        INTEGER: "Int32",
        FINITE_FLOAT: "Float64",
        NON_NEGATIVE_FLOAT: "Float64",
        BOOL: "UInt8",
    }
)


def _identity_mappings(
    dataset_name: str,
    *,
    excluded_fields: tuple[str, ...] = (),
) -> tuple[FieldMapping, ...]:
    """Declare unchanged logical-to-serving fields without hiding their types."""

    contract = DATASET_CONTRACTS_BY_NAME[dataset_name]
    return tuple(
        _mapping(
            dataset_name,
            field.name,
            serving_field=field.name,
            serving_type=_SERVING_TYPES[field.logical_type],
            conversion="Identity field with validated compatible physical type.",
        )
        for field in contract.fields
        if field.name not in excluded_fields
    )


_SALES_DAILY_MAPPING = ServingDatasetMapping(
    logical_dataset="sales_daily",
    serving_dataset="sales_daily",
    logical_key=SALES_DAILY.unique_key,
    field_mappings=_identity_mappings(
        "sales_daily", excluded_fields=("date", "unit_sales", "series_id", "day_key", "wm_yr_wk")
    )
    + (
        _mapping(
            "sales_daily",
            "date",
            serving_field="sale_date",
            serving_type="Date",
            conversion="Explicit logical date to serving sale_date rename.",
        ),
        _mapping(
            "sales_daily",
            "unit_sales",
            serving_field="unit_sales",
            serving_type="UInt32",
            conversion="Validated nonnegative integer cast; no semantic rename.",
            query_aliases=("units_sold",),
        ),
    ),
    lineage_fields=("source_snapshot_id",),
    omitted_fields=("series_id", "day_key", "wm_yr_wk"),
    metadata_location="Omitted fields remain in curated Parquet and manifest metadata.",
)


_STORE_DAILY_MAPPING = ServingDatasetMapping(
    logical_dataset="store_daily_sales",
    serving_dataset="store_daily_sales",
    logical_key=STORE_DAILY_SALES.unique_key,
    field_mappings=_identity_mappings("store_daily_sales", excluded_fields=("date", "total_units"))
    + (
        _mapping(
            "store_daily_sales",
            "date",
            serving_field="sale_date",
            serving_type="Date",
            conversion="Explicit logical date to serving sale_date rename.",
        ),
        _mapping(
            "store_daily_sales",
            "total_units",
            serving_field="total_units",
            serving_type="UInt64",
            conversion="Validated aggregate integer cast; units_sold is query alias only.",
            query_aliases=("units_sold",),
        ),
    ),
    lineage_fields=("source_snapshot_id",),
    omitted_fields=(),
    metadata_location="Source snapshot and aggregate scope remain in the row and publication manifest.",
)


_FORECAST_DAILY_MAPPING = ServingDatasetMapping(
    logical_dataset="forecast_daily",
    serving_dataset="forecast_daily",
    logical_key=FORECAST_DAILY.unique_key,
    field_mappings=_identity_mappings(
        "forecast_daily", excluded_fields=("raw_prediction", "predicted_units")
    )
    + (
        _mapping(
            "forecast_daily",
            "raw_prediction",
            serving_field=None,
            serving_type=None,
            conversion="No conversion; retain the finite raw model output in Parquet.",
            retention_location="Parquet forecast artifact; serving retention requires explicit approval.",
            approval_required=True,
        ),
        _mapping(
            "forecast_daily",
            "predicted_units",
            serving_field="predicted_units",
            serving_type="Float64",
            conversion="Validated finite nonnegative Float64; do not round before decisions.",
        ),
    ),
    lineage_fields=(
        "source_snapshot_id",
        "model_run_id",
        "forecast_run_id",
        "feature_version",
    ),
    omitted_fields=("raw_prediction",),
    metadata_location="All other logical forecast fields are retained in serving; raw_prediction remains Parquet pending approval.",
    extra_serving_fields=("forecast_status",),
)


_INVENTORY_SCENARIO_MAPPING = ServingDatasetMapping(
    logical_dataset="inventory_scenario",
    serving_dataset="inventory_scenario",
    logical_key=INVENTORY_SCENARIO.unique_key,
    field_mappings=_identity_mappings("inventory_scenario"),
    lineage_fields=("source_snapshot_id", "scenario_id"),
    omitted_fields=(),
    metadata_location="Scenario generation metadata is retained in the scenario manifest.",
)


_CENTRAL_SUPPLY_MAPPING = ServingDatasetMapping(
    logical_dataset="central_supply_scenario",
    serving_dataset="central_supply_scenario",
    logical_key=CENTRAL_SUPPLY_SCENARIO.unique_key,
    field_mappings=_identity_mappings("central_supply_scenario"),
    lineage_fields=("scenario_id",),
    omitted_fields=(),
    metadata_location="Central supply remains one product-level simulated row.",
)


_REPLENISHMENT_MAPPING = ServingDatasetMapping(
    logical_dataset="replenishment_recommendations",
    serving_dataset="replenishment",
    logical_key=REPLENISHMENT_RECOMMENDATIONS.unique_key,
    field_mappings=_identity_mappings("replenishment_recommendations"),
    lineage_fields=(
        "source_snapshot_id",
        "model_run_id",
        "forecast_run_id",
        "scenario_id",
        "decision_run_id",
        "policy_version",
    ),
    omitted_fields=(),
    metadata_location="Logical recommendation identity is preserved while the serving table uses replenishment.",
)


_ALLOCATION_MAPPING = ServingDatasetMapping(
    logical_dataset="allocation_recommendations",
    serving_dataset="allocation",
    logical_key=ALLOCATION_RECOMMENDATIONS.unique_key,
    field_mappings=_identity_mappings("allocation_recommendations"),
    lineage_fields=(
        "source_snapshot_id",
        "model_run_id",
        "forecast_run_id",
        "scenario_id",
        "decision_run_id",
        "policy_id",
        "policy_version",
    ),
    omitted_fields=(),
    metadata_location="Logical recommendation identity is preserved while the serving table uses allocation.",
)


_FORECAST_METRICS_MAPPING = ServingDatasetMapping(
    logical_dataset="forecast_metrics",
    serving_dataset="forecast_metrics",
    logical_key=FORECAST_METRICS.unique_key,
    field_mappings=_identity_mappings("forecast_metrics"),
    lineage_fields=("evaluation_run_id", "model_run_id", "forecast_run_id", "holdout_id"),
    omitted_fields=(),
    metadata_location="Metric cohort, horizon, and nullable undefined values remain explicit in the serving contract.",
)


_POLICY_EVALUATION_MAPPING = ServingDatasetMapping(
    logical_dataset="inventory_policy_evaluation",
    serving_dataset="policy_evaluation",
    logical_key=INVENTORY_POLICY_EVALUATION.unique_key,
    field_mappings=_identity_mappings("inventory_policy_evaluation"),
    lineage_fields=(
        "evaluation_run_id",
        "source_snapshot_id",
        "forecast_run_id",
        "scenario_id",
        "decision_run_id",
        "policy_id",
        "policy_version",
    ),
    omitted_fields=(),
    metadata_location="Historical policy outcomes remain separate from forecast metrics.",
)


SERVING_DATASET_MAPPINGS: Final = (
    _SALES_DAILY_MAPPING,
    _STORE_DAILY_MAPPING,
    _FORECAST_DAILY_MAPPING,
    _INVENTORY_SCENARIO_MAPPING,
    _CENTRAL_SUPPLY_MAPPING,
    _REPLENISHMENT_MAPPING,
    _ALLOCATION_MAPPING,
    _FORECAST_METRICS_MAPPING,
    _POLICY_EVALUATION_MAPPING,
)

FIELD_MAPPINGS: Final = tuple(
    field_mapping
    for dataset_mapping in SERVING_DATASET_MAPPINGS
    for field_mapping in dataset_mapping.field_mappings
)
