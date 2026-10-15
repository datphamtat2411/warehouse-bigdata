# 02 — System Requirements Specification (SRS)

> **Project:** Big Data–Driven Inventory and Distribution Optimization  
> **Original title:** Tối ưu hóa hệ thống quản lý kho hàng  
> **Course:** Introduction to Big Data  
> **Document type:** Software Requirements Specification (SRS)  
> **Status:** Proposed requirements baseline — subject to implementation validation  
> **Version:** 1.0 — October 15, 2026  
> **Related document:** [01 — Project Overview](01_PROJECT_OVERVIEW.md)

## 1. Purpose and Document Boundaries

This document defines **what the system must do, what conditions it must satisfy, and how compliance will be verified**. It is the team's requirements baseline for design, implementation, testing, reporting, and defense.

This is **not** a business-rule specification, infrastructure runbook, database schema, model-training tutorial, or UI wireframe. Those details belong in the corresponding design documents. For example:

- **This SRS:** The system shall recommend a nonnegative replenishment quantity for a selected product and store.
- **Business rules document:** Defines the replenishment equation, units, assumptions, priority policy, and edge cases.
- **Database design document:** Defines where the recommendations are stored and how they are queried.
- **Application design document:** Defines the controls and charts used to display the recommendation.
- **Testing document:** Defines executable tests and captures the actual results.

### 1.1. Normative terminology

| Term | Meaning |
|---|---|
| **MUST / shall** | Required for the agreed project scope. |
| **SHOULD** | Strongly preferred; deviations must be explained. |
| **MAY** | Optional; not necessary for core acceptance. |
| **Acceptance criterion** | Observable evidence that a requirement has been met. |
| **Baseline** | A simple, documented alternative used for a fair comparison. |
| **Scenario / simulated data** | Operational data explicitly constructed for experimentation, **not** sourced from Walmart. |

**Status rule:** All items below describe **planned behavior**, not features already implemented or results already measured. Test and benchmark results must be filled in only after execution.

## 2. System Context and Intended Outcomes

### 2.1. System definition

Build an academic **batch-oriented inventory decision-support prototype** that processes the historical M5 Walmart dataset, forecasts store-level product sales over a seven-day horizon, and uses those forecasts to support simulated inventory replenishment and constrained stock allocation.

The project shall demonstrate real distributed data storage, distributed processing, distributed analytical database queries, and an evaluated machine-learning workflow. It shall expose the results through a simple **Vietnamese-language** Streamlit dashboard.

### 2.2. Primary users and stakeholders

| ID | Actor / stakeholder | Primary interest |
|---|---|---|
| ACT-01 | Inventory manager (demo user) | Inspect demand forecasts and proposed replenishment quantities. |
| ACT-02 | Distribution coordinator (demo user) | Understand how limited stock is allocated among stores. |
| ACT-03 | Project team (two students) | Run, inspect, test, and explain the system and its evidence. |
| ACT-04 | Instructor / evaluator | Verify technical authenticity, data provenance, ML evaluation, visualization, and the rubric. |

**Authentication and role-based permissions are not required:** actors describe usage perspectives, not separate login accounts.

### 2.3. Core use cases

| ID | Use case | Expected outcome |
|---|---|---|
| UC-01 | Explore historical sales | Filter and visualize sales by date, store, product, or category. |
| UC-02 | View a seven-day forecast | Select an eligible product–store pair and view daily forecasts and their total. |
| UC-03 | Review replenishment | Combine the forecast with a labeled simulated inventory scenario to calculate needed stock. |
| UC-04 | Review constrained allocation | Show a feasible allocation of available central stock to stores and unmet requests if any. |
| UC-05 | Compare decisions | Inspect forecast quality and policy performance against documented baselines. |
| UC-06 | Verify Big Data technologies | Inspect HDFS placement, Spark execution, and ClickHouse sharding with actual evidence. |

## 3. Scope and Requirement Priority

### 3.1. In scope

- Import and document the M5 dataset; store raw and curated data in HDFS.
- Use Apache Spark to transform, validate, aggregate, and prepare historical sales data.
- Train and evaluate a **Spark MLlib Random Forest Regressor**, producing seven-day sales forecasts for a clearly defined eligible population.
- Generate **explicitly simulated** inventory states and central-warehouse supply assumptions.
- Compute replenishment and feasible allocation recommendations with explainable rules.
- Store and query application-serving analytical data in a **two-shard ClickHouse Distributed cluster**.
- Present four focused Streamlit views in **Vietnamese**.
- Produce reproducible execution evidence, model metrics, decision comparisons, and a demo-ready deliverable.

### 3.2. Priority convention

- **P0 — Essential:** Core project acceptance and evidence required for the demo.
- **P1 — Important:** Improves clarity, operational quality, or evaluation; should be completed after P0.
- **P2 — Optional:** Nice to have and must not displace P0/P1 work.

No P0 requirement should be weakened merely to make the demo easier. Conversely, avoid adding architectural components without a measurable purpose.

## 4. Functional Requirements (FR)

Each requirement uses the following structure: **ID → obligation → acceptance criteria**. Implementation choices are intentionally kept out of the acceptance rules unless they are explicit course or team constraints.

### 4.1. Data ingestion and distributed processing

#### FR-01 — Identify and register the source dataset (**P0**)

**Requirement:** The system/project shall use the publicly identified **M5 Forecasting — Accuracy** dataset and shall document the origin, files, column meanings, observation grain, and actual downloaded/processed sizes.

**Acceptance criteria:**
1. The official Kaggle dataset URL and input filenames are recorded.
2. The distinction between **product–store–day observations** and independent transactions is stated clearly.
3. Recorded source and output row counts are reproducible from executed queries/scripts.

#### FR-02 — Ingest source data into distributed storage (**P0**)

**Requirement:** The project shall place the required source files in **Hadoop HDFS** and shall read source or curated HDFS paths as part of the Spark pipeline.

**Acceptance criteria:**
1. HDFS exposes the project input files in documented directories.
2. At least two configured DataNodes are visible, with actual stored files/blocks.
3. An actual Spark job reads input data via HDFS, rather than silently substituting a local-file path.

#### FR-03 — Clean, normalize, and validate data with Spark (**P0**)

**Requirement:** Spark shall perform the necessary quality checks and convert the original wide daily sales structure to a documented analytical form suitable for downstream analysis.

**Acceptance criteria:**
1. A repeatable Spark job executes the transformation and writes curated results (preferably Parquet) to HDFS.
2. Data types, missing values, key validity, duplication assumptions, and invalid records are examined and summarized.
3. For an **unfiltered full reshape** of `sales_train_evaluation.csv`, the anticipated count is **30,490 × 1,941 = 59,181,090** product–store–day observations; actual counts and deviations are documented.
4. The job records input/output counts so filtered or aggregated datasets cannot be confused with the full historical set.

#### FR-04 — Provide historical sales analytics (**P0**)

**Requirement:** The system shall calculate usable historical summaries by product, store, and time, with category-level summaries where the source attributes permit.

**Acceptance criteria:**
1. At least daily/weekly sales trends and store/product breakdowns are available to the application.
2. Sample aggregate results can be independently reconciled with the curated dataset.
3. Grouping/filter rules and the date period are documented.

### 4.2. Forecasting and machine learning

#### FR-05 — Construct forecasting-ready features (**P0**)

**Requirement:** The system shall prepare valid time-aware features from historical sales and available calendar/price/product/store information, without using future observations to predict earlier dates.

**Acceptance criteria:**
1. The feature dataset documents each feature, source, transformation, and missing-value behavior.
2. Lag/rolling features are computed using data available at the forecast origin.
3. Feature-generation tests or inspections rule out obvious temporal leakage.

#### FR-06 — Train and persist the specified ML model (**P0**)

**Requirement:** The system shall train a **Random Forest Regression model through Spark MLlib** and persist the model artifact and training configuration.

**Acceptance criteria:**
1. Executed code demonstrates the use of Spark MLlib's Random Forest regression implementation.
2. Training input coverage, data period, sample/series count, parameters, and reproducibility settings are recorded.
3. The persisted model can be loaded for inference without retraining through the UI.

#### FR-07 — Evaluate the forecasting model against a baseline (**P0**)

**Requirement:** The system shall evaluate forecasts on chronologically later observations and compare them with at least one simple, documented forecasting baseline.

**Acceptance criteria:**
1. Train/validation/test or backtesting windows are explicitly chronological and non-overlapping where required.
2. The same evaluation set and forecasting horizon are used for the model and baseline.
3. **MAE, RMSE, and WAPE** are reported with appropriate definitions and a safe treatment of zero denominators.
4. The report includes measured values even if Random Forest does **not** outperform the baseline.

#### FR-08 — Produce seven-day predictions (**P0**)

**Requirement:** For each eligible selected product–store pair, the system shall expose **seven daily nonnegative sales forecasts**, their date labels, and their seven-day total.

**Acceptance criteria:**
1. Exactly seven forecast dates and values appear for a supported selection and forecast origin.
2. Units are identified as predicted **unit sales**, not real on-hand inventory or guaranteed unconstrained demand.
3. The forecast period, origin/cutoff, supported item–store coverage, and any missing coverage are visible or documented.
4. The dashboard displays a clear notice rather than fabricating output for an unsupported selection.

### 4.3. Inventory simulation and decision support

#### FR-09 — Create reproducible simulated inventory scenarios (**P0**)

**Requirement:** Because M5 lacks actual stock records, the project shall provide explicitly identified, reproducible simulated values for on-hand inventory, central available stock, and necessary policy parameters.

**Acceptance criteria:**
1. Every simulated field is labeled as simulated in stored metadata and user-facing explanations where relevant.
2. Scenario parameters and deterministic generation rules or seeds are recorded.
3. The same inputs and scenario version yield the same decision outputs.
4. Scenario data are never attributed to Walmart as actual inventory observations.

#### FR-10 — Recommend replenishment quantities (**P0**)

**Requirement:** For an eligible product–store pair, the system shall calculate a **nonnegative replenishment recommendation** based on forecast demand and the active inventory scenario.

**Acceptance criteria:**
1. The result identifies the selected product, store, planning horizon, forecast quantity, assumed available inventory, and recommended replenishment quantity.
2. Under the documented business rule, inventory sufficient to cover the target quantity produces no unnecessary replenishment recommendation.
3. Inputs and outputs use consistent unit and time conventions.
4. The UI explains that this is a **decision-support recommendation**, not an order placed with a real supplier.

#### FR-11 — Allocate constrained central stock to stores (**P0**)

**Requirement:** The system shall produce a feasible, explainable proposed allocation when several stores request limited central stock for the same product.

**Acceptance criteria:**
1. Each store allocation is a nonnegative quantity and does not exceed its eligible requested quantity.
2. The sum of allocations does not exceed the central stock available for that product/scenario.
3. The allocation output includes unmet quantities when requests exceed available stock.
4. Allocation rules, eligibility assumptions, rounding/integer treatment, and tie-breaking are documented in the business-rules specification.

#### FR-12 — Compare inventory decisions against a baseline (**P0**)

**Requirement:** The project shall compare the proposed replenishment/allocation policy with at least one documented alternative under **identical historical periods and simulation assumptions**.

**Acceptance criteria:**
1. Baseline and proposed policies receive comparable available inputs and constraints.
2. At least **unmet demand/stockout proxy, remaining stock or overstock proxy, and demand fulfillment rate** are calculated and explained.
3. Results distinguish observed historical sales from simulated operational outcomes.
4. No unmeasured claim of actual Walmart savings or production improvement is made.

### 4.4. Distributed analytical database and application

#### FR-13 — Serve data through a genuinely sharded ClickHouse cluster (**P0**)

**Requirement:** Analytical datasets required by the application shall be persisted in and queried from a **ClickHouse cluster with two active shards**, using ClickHouse's distributed-query mechanism.

**Acceptance criteria:**
1. Both shard nodes hold **real, nonempty project-derived data**; a collection of empty containers or duplicate demo-only rows is insufficient.
2. Querying the distributed table aggregates data across the expected shards; per-shard and combined counts reconcile for the loaded dataset.
3. The loaded dataset's **grain, date range, and coverage** are recorded. A filtered/aggregated load must not be described as all 59.18 million raw observations.
4. The cluster configuration clearly distinguishes **two shards with one replica each** from replicated high availability.

#### FR-14 — Provide a historical overview dashboard (**P0**)

**Requirement:** The application shall provide a Vietnamese-language **“Tổng quan”** view containing key dataset/analysis indicators, at least one historical trend chart, and useful filters.

**Acceptance criteria:**
1. Indicators and plots use processed/project analytical data rather than manually typed example numbers.
2. Filters have defined behavior and the displayed date range and measurement units are clear.
3. Empty or unsupported filters produce understandable Vietnamese feedback.

#### FR-15 — Provide a forecast dashboard (**P0**)

**Requirement:** The application shall provide a Vietnamese-language **“Dự báo nhu cầu”** view for inspecting the outputs of FR-08.

**Acceptance criteria:**
1. A user can select an eligible product and store.
2. The view displays seven-day predictions in a readable chart/table and shows the total.
3. A compact evaluation summary or a clear path to model-performance information is available.

#### FR-16 — Provide a replenishment dashboard (**P0**)

**Requirement:** The application shall provide a Vietnamese-language **“Tồn kho”** view showing the active simulated stock assumptions and the replenishment recommendations produced under FR-10.

**Acceptance criteria:**
1. Simulated versus source-derived or predicted values are visually distinguishable.
2. The view identifies products/stores needing stock and shows the recommended quantities.
3. Any supported scenario inputs can be changed without silently changing the underlying M5 sales history.

#### FR-17 — Provide a distribution dashboard (**P0**)

**Requirement:** The application shall provide a Vietnamese-language **“Phân bổ hàng”** view presenting the proposed distribution of limited central stock and its unmet requests.

**Acceptance criteria:**
1. A user can inspect allocations by product and store for the chosen scenario.
2. The page exposes central stock availability, total requested quantity, total allocated quantity, and any shortfall.
3. The allocation view is understandable without exposing internal database or ML implementation details.

#### FR-18 — Expose verifiable project evidence (**P0**)

**Requirement:** The deliverable shall include inspectable evidence that the stated technologies and models were actually used and that the main outputs can be regenerated.

**Acceptance criteria:**
1. HDFS block/file placement and DataNode status are observable.
2. Spark jobs, stages/tasks, and Worker/Executor activity are observable for executed jobs.
3. ClickHouse shard metadata, per-shard counts, and distributed queries are demonstrable.
4. ML metrics, baseline results, and inventory comparisons originate from executed experiments.
5. The repository/report identifies how to reproduce the core workflow and separates measured results from planned values.

### 4.5. Important, non-core enhancements

| ID | Priority | Requirement | Verification |
|---|---|---|---|
| FR-19 | P1 | Export selected forecast, replenishment, or allocation tables as CSV. | Exported rows/columns match the visible filtered results. |
| FR-20 | P1 | Provide explicit pipeline/model run metadata (timestamps, versions, status). | Users can identify which batch produced the displayed results. |
| FR-21 | P1 | Present a concise comparison of at least two Spark processing configurations. | Same workload and dataset scope; measured time/configuration recorded. |
| FR-22 | P2 | Support additional scenario presets for different levels of central stock scarcity. | Outputs remain deterministic and constraint-compliant. |

## 5. Data Requirements (DR)

These rules specify **data meaning, lineage, and integrity**, not physical table definitions.

| ID | Priority | Data requirement | Acceptance criterion |
|---|---|---|---|
| DR-01 | P0 | Retain **M5 source provenance** for every original dataset. | Kaggle link, file name, extraction/ingestion date, and measured file size are recorded. |
| DR-02 | P0 | Define the unit of observation. | Long-format sales grain is one `item_id` × `store_id` × day; zero-sale rows are handled deliberately. |
| DR-03 | P0 | Maintain correct joins to calendar and price data. | Join-key uniqueness/cardinality and missing-price behavior are checked and documented. |
| DR-04 | P0 | Separate **observed**, **derived**, **predicted**, and **simulated** data. | Lineage/field descriptions and output labels identify each category. |
| DR-05 | P0 | Do not present forecast output as real customer orders or true unconstrained demand. | Reports/UI label values as predicted unit sales and note source limitations. |
| DR-06 | P0 | Maintain temporal consistency and prevent future-data leakage. | Historical cutoffs, feature timestamps, and prediction dates are traceable. |
| DR-07 | P0 | Clearly declare the grain and scope of each ClickHouse-loaded dataset. | Loaded record counts can be reconciled against precisely defined Spark outputs. |
| DR-08 | P0 | Ensure operational quantities have consistent units and valid domains. | Invalid negative stock/allocation, unsupported items, or malformed scenario values are rejected or handled explicitly. |
| DR-09 | P1 | Capture run-level lineage for reproducibility. | A result can be traced to dataset version, pipeline run, model run, and scenario version. |

### 5.1. Reference data-scale statement

The M5 evaluation training file represents **30,490 product–store series** with **1,941 daily sales columns**. A full unfiltered wide-to-long transformation therefore yields **59,181,090 product–store–day observations**. The count is a **derived observation count**, not 59 million independent transactions or different products. Actual implementation counts must still be measured and audited.

### 5.2. Coverage transparency

The pipeline shall process and validate the intended historical M5 scope. The **model-training population** and **ClickHouse serving population** may be reduced or aggregated for technical reasons on the 16 GB host **only if** the reduction is justified, reproducible, and explicitly disclosed. The demo must never claim comprehensive forecast or database coverage that was not actually produced.

## 6. Non-Functional Requirements (NFR)

| ID | Priority | Category | Requirement / measurable evidence |
|---|---|---|---|
| NFR-01 | P0 | Distributed-storage authenticity | HDFS stores real project data on an active NameNode/DataNode arrangement; at least two DataNodes are visible and their data/block status can be inspected. |
| NFR-02 | P0 | Distributed-compute authenticity | Spark jobs execute using a configured Master and at least two Workers; execution evidence includes actual stages, tasks, and executor/worker activity rather than only installed services. |
| NFR-03 | P0 | Distributed-query authenticity | ClickHouse operates with two active, nonempty shards and a distributed table/query path; combined query results reconcile with the loaded shard data. |
| NFR-04 | P0 | Data correctness | Transformation totals, sample aggregates, forecast dates, and stock/allocation constraints pass documented validation checks. |
| NFR-05 | P0 | Reproducibility | Source/configuration identifiers, random seeds where applicable, model parameters, and scenario parameters are documented so another team member can rerun the workflow. |
| NFR-06 | P0 | Resource compatibility | The solution can be demonstrated on a **16 GB RAM Windows host using Ubuntu under WSL2**, using bounded container/service resources and separate heavy batch phases where needed. |
| NFR-07 | P0 | Application clarity and language | All user-facing Streamlit labels, descriptions, navigation, warnings, and chart captions are **Vietnamese**; engineering docs, code, comments, and database identifiers remain **English**. |
| NFR-08 | P0 | Failure transparency | Missing forecasts, empty query results, unavailable services, and invalid scenario inputs produce explicit feedback; the application must not silently substitute fabricated success data. |
| NFR-09 | P0 | Honest evaluation | Forecasting and optimization claims are supported by measured tests; negative or inconclusive results are retained and discussed. |
| NFR-10 | P1 | Interactive responsiveness | On the configured demo environment, record elapsed times for common filtered dashboard operations; initial engineering target: **≤ 10 seconds** for a warm, typical filtered query. Treat this as a target to benchmark, not a pre-verified claim. |
| NFR-11 | P1 | Maintainability | Separate ingestion, transformation, ML, business policies, database access, and presentation code into inspectable modules with descriptive English names. |
| NFR-12 | P1 | Verification portability | Another team member can reproduce the standard test/demo sequence from repository instructions and available artifacts without hidden manual data editing. |

**Infrastructure precision:** A multi-container cluster on one physical machine demonstrates **distributed software execution**, **not** physical host fault tolerance. A two-shard, single-replica ClickHouse cluster demonstrates sharding, **not** high availability. These limitations shall be stated in the report and defense.

## 7. Constraints, Assumptions, and External Dependencies

### 7.1. Project constraints

| ID | Constraint | Consequence |
|---|---|---|
| CON-01 | Primary dataset is M5 Forecasting — Accuracy. | The project cannot claim availability of real inventory, warehouse, transportation, or supplier records. |
| CON-02 | Required Big Data components are HDFS, Apache Spark, and ClickHouse Distributed (two shards). | Each must fulfill a genuine storage, computing, or query-serving role and be demonstrable. |
| CON-03 | Required main forecasting approach is Spark MLlib Random Forest Regression. | Alternative models may serve as baselines but do not replace the agreed main model without a documented decision. |
| CON-04 | Forecasting horizon is **seven days**. | Forecast visualizations, replenishment planning, and evaluation must use consistent time conventions. |
| CON-05 | Application stack is Streamlit + Plotly. | The project is a focused analytical prototype, not a full enterprise WMS. |
| CON-06 | Hardware is a Windows machine with **16 GB physical RAM**, using Ubuntu in WSL2. | Memory limits, batch scheduling, and profiling are necessary; full simultaneous heavy workloads are not required. |
| CON-07 | Engineering artifacts are in **English**, and the end-user interface is in **Vietnamese**. | UI text must be localized; code/database identifiers need not be translated. |
| CON-08 | Team size is two students. | Responsibilities and actual contributions must be captured in final academic materials. |

### 7.2. Assumptions that require disclosure

- On-hand stock, safety stock, central stock availability, and similar operational values are **simulated scenario variables**.
- M5's recorded sales are used as a **proxy for observed sales demand**, with potential stockout censoring unknown.
- Distribution means **allocating quantities** among stores, not optimizing truck routes or delivery distances.
- Forecasts may be generated for a disclosed subset of eligible item–store pairs if warranted by resource limits; unserved pairs must be reported as unavailable.
- Historical evaluation/backtests and simulated decisions are **not** evidence of live Walmart business improvement.
- The actual WSL2 and Docker memory footprint must be measured on the development machine before claiming operational capacity.

### 7.3. External dependencies

- Authorized access to the M5 Kaggle dataset and compliance with its access/redistribution conditions.
- A compatible installation of Python, Java/Hadoop, Apache Spark, ClickHouse, Docker/Compose, and application dependencies.
- Sufficient local storage for original CSVs, curated Parquet outputs, ClickHouse data, artifacts, logs, and test runs.

Specific versions, ports, memory quotas, deployment commands, schema layouts, sharding keys, and algorithms are **design details** and belong in other documents.

## 8. Out of Scope

The following items are explicitly **not required** for this academic prototype:

- Commercial ERP/WMS modules, user registration, payments, invoicing, or accounting.
- Real-time inventory ingestion, streaming platforms, or direct connections to Walmart systems.
- Real warehouse stock verification, physical warehouse operations, supplier integrations, or automatic purchase order submission.
- Route optimization, vehicle scheduling, geospatial delivery planning, or delivery-time predictions.
- Production multi-host disaster recovery or high availability via multiple ClickHouse replicas.
- Additional distributed systems such as Kafka or Kubernetes solely to increase the apparent complexity.
- A claim that Random Forest is universally superior or that the model will achieve an accuracy/stockout target before experiments.

## 9. End-to-End Acceptance Scenarios

The following scenarios join related requirements into testable project outcomes. Expected outputs are **criteria**, not completed test results.

| Scenario | Preconditions | Expected result | Requirement links |
|---|---|---|---|
| AT-01 — Provenance and row-count audit | M5 files are available. | Official source documented; full reshape count checked; wide/long grain explained. | FR-01, FR-03, DR-01, DR-02 |
| AT-02 — HDFS and Spark proof | Cluster started; source input loaded. | HDFS holds files on active DataNodes; Spark executes transformations on Workers and writes outputs. | FR-02, FR-03, NFR-01, NFR-02 |
| AT-03 — Seven-day model evaluation | Forecasting feature data and chronological test window available. | Persisted Random Forest produces seven-day output; baseline and MAE/RMSE/WAPE measured without leakage. | FR-05 to FR-08, DR-06 |
| AT-04 — Stock replenishment sanity check | Scenario demand = 80, safety stock = 10, available inventory = 50; same planning period. | Proposed replenishment = **40** units under the documented illustrative rule; adequate-stock cases result in zero. | FR-09, FR-10 |
| AT-05 — Limited-stock allocation | Sum of requests exceeds central stock. | Allocated quantities are nonnegative, no store gets more than requested, total allocated ≤ supply; unmet requests shown. | FR-11, DR-08 |
| AT-06 — Policy comparison | Identical backtest interval and scenario inputs. | At least one baseline comparison reports declared metrics and assumptions, whether favorable or not. | FR-12, NFR-09 |
| AT-07 — ClickHouse sharding proof | Two nonempty shards with project-derived analytical data. | Per-shard counts and distributed-table count reconcile; a distributed aggregate returns correct values. | FR-13, NFR-03 |
| AT-08 — Four-view Vietnamese demo | Batch outputs are available in serving datasets. | All four named views open, filter/query data, and show clear Vietnamese labels; unsupported selections receive explicit feedback. | FR-14 to FR-17, NFR-07, NFR-08 |
| AT-09 — Reproducibility and defense | Code, config, and experiment outputs available. | A teammate can explain and replay the workflow and show HDFS, Spark, ClickHouse, ML, and decision evidence. | FR-18, NFR-05, NFR-12 |

**AT-04 is a controlled rule test, not a claim of real Walmart demand or inventory.** The precise replenishment equation and assumptions must be documented in the business-rules document before implementation.

## 10. Requirements Traceability to Design and Evidence

The SRS owns the **required behavior**. The linked documents own **how it is designed and verified**.

| Requirement group | Primary design source | Verification / evidence source |
|---|---|---|
| FR-01, DR-01–DR-03 | [04 — Dataset](04_DATASET.md) | Dataset profiling, source URL, counts and join-quality checks |
| FR-02–FR-04, NFR-01–NFR-02 | [06 — Data Pipeline](06_DATA_PIPELINE.md), [03 — Architecture](03_SYSTEM_ARCHITECTURE.md) | HDFS/Spark UIs, execution logs, reconciliation tests |
| FR-05–FR-08, DR-06 | [07 — Machine Learning](07_MACHINE_LEARNING.md) | Chronological holdout, model artifact, baseline, metrics |
| FR-09–FR-12, DR-04–DR-05, DR-08 | [08 — Business Rules & Optimization](08_BUSINESS_RULES_AND_OPTIMIZATION.md) | Scenario fixtures, rule/constraint tests, policy backtests |
| FR-13, DR-07, NFR-03 | [09 — Database Design](09_DATABASE_DESIGN.md) | ClickHouse cluster topology, shard queries and count audits |
| FR-14–FR-17, NFR-07–NFR-08 | [10 — Application Design](10_APPLICATION_DESIGN.md) | Four-view functional demo, screenshots, empty/error-state tests |
| NFR-06, NFR-10–NFR-12 | [03 — Architecture](03_SYSTEM_ARCHITECTURE.md), [05 — Tech Stack](05_TECH_STACK.md) | Resource monitoring, test runs, application response-time measurements |
| FR-18, AT-01–AT-09 | [11 — Testing & Demo](11_TESTING_AND_DEMO.md) | Test cases, recorded observations, evidence directory, defense script |

### 10.1. Academic rubric coverage

| Evaluation area | Max points | Requirements / expected evidence |
|---|---:|---|
| Practical applicability | 1.0 | UC-02–UC-05, FR-09–FR-12; meaningful replenishment and allocation outputs |
| Distributed data storage and processing | 2.0 | FR-02–FR-04, NFR-01–NFR-03; authentic HDFS/Spark/ClickHouse operation |
| Processing model / technology | 2.5 | FR-05–FR-08 and FR-12; implemented ML, evaluation, and justified decisions |
| Visualization / application | 2.5 | FR-14–FR-17; working Vietnamese-language dashboard with informative results |
| Written report and presentation | 1.0 | FR-18; coherent methodology, results, limitations and reproducible evidence |
| Teamwork and division of responsibility | 1.0 | CON-08; documented contributions, collaborative source control, and team evidence |
| **Total** | **10.0** | Scoring still depends on implementation and the instructor's assessment. |

## 11. Change Control and Definition of Done

### 11.1. Requirement changes

When a requirement changes, the team shall record:

1. The requirement ID and previous/new wording.
2. The reason (data limitations, evidence, instructor feedback, or measured resource constraints).
3. The impact on architecture, dataset coverage, tests, and demo.
4. Whether related design documents and acceptance scenarios need updates.

A reduction in model/data coverage is permissible only when **explicitly documented and reflected in acceptance evidence**, not silently hidden behind the user interface.

### 11.2. Core project Definition of Done

The academic prototype is considered ready for final evaluation when:

- All **P0 functional requirements** have either passed their acceptance checks or have a clearly disclosed, instructor-acceptable deviation.
- All **P0 NFRs** have supporting test or execution evidence.
- Data source and derived row counts are auditable; simulated fields are clearly labeled.
- The seven-day model is evaluated against a baseline using chronological data.
- Replenishment/allocation constraints and comparative decision metrics are verified.
- HDFS, Spark, and the two-shard ClickHouse cluster are demonstrably functional.
- The **four Vietnamese UI views** display actual generated analysis/results and handle unavailable selections honestly.
- The deliverable includes code, references to data, documentation, report/slides, run instructions, and documented contributions for both students.

**Completion is not the same as a perfect score:** the team's obligation is to demonstrate a correct, evidence-based, technically meaningful implementation, not to promise outcomes that have not been measured.

## 12. References and Related Documents

- [01 — Project Overview](01_PROJECT_OVERVIEW.md): business context, goals, boundaries, and overall solution.
- [03 — System Architecture](03_SYSTEM_ARCHITECTURE.md): components, data flow, deployment topology, and design trade-offs.
- [04 — Dataset](04_DATASET.md): detailed M5 files, fields, data quality, joins, and profiling.
- [05 — Tech Stack](05_TECH_STACK.md): technology selection, versions, compatibility, and rationale.
- [06 — Data Pipeline](06_DATA_PIPELINE.md): ETL stages, transformations, partitioning, and HDFS outputs.
- [07 — Machine Learning](07_MACHINE_LEARNING.md): forecasting strategy, feature engineering, training, baseline, and metrics.
- [08 — Business Rules and Optimization](08_BUSINESS_RULES_AND_OPTIMIZATION.md): replenishment formula, allocation policy, constraints, and experiments.
- [09 — Database Design](09_DATABASE_DESIGN.md): ClickHouse schema, two-shard routing, and distributed queries.
- [10 — Application Design](10_APPLICATION_DESIGN.md): Vietnamese UI behavior, layouts, filters, and visualizations.
- [11 — Testing and Demo](11_TESTING_AND_DEMO.md): detailed test cases, measurements, technical demonstrations, and defense evidence.
- **Primary dataset:** [Kaggle — M5 Forecasting Accuracy](https://www.kaggle.com/competitions/m5-forecasting-accuracy/data).

> **Document boundary:** Requirements say **what must be true**. The other documents specify **how to make it true**, and testing supplies **proof that it became true**.
