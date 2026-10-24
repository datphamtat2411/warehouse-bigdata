# Task Plan

Implement **M09 Historical Overview Dashboard** as a bounded, publication-pinned Vietnamese presentation of observed M5 unit sales.

Acceptance: approved filters drive one validated daily dataset; the filtered-sales KPI, Plotly chart, and detail table reconcile exactly. Coverage indicators remain distinct from filtered sales.

**Planning blocker:** the current application contracts do not expose confirmed loaded-history boundaries or a measured loaded-history day count. Resolve this upstream prerequisite before implementing date controls and history-coverage KPIs. Do not infer these measurements from the forecast issue date or hard-code the standard dataset population.

This is the proposed `plan.md` content; no repository files were modified.

# Repository Findings

- `app/shell.py` validates approval on every render, captures one publication context, opens a scoped repository, and passes shared selections to the destination.
- `app/views.py::render_overview` is currently a placeholder. `render_read_state` already provides safe Vietnamese feedback for `ReadResult` failure states.
- `application/session_state.py` owns publication invalidation and shared selections. `set_selection` resets the selected product when the store changes; publication changes clear all `m09.*` state except navigation.
- `ClickHouseReadRepository` provides the required reads:
  - `get_store_trend(store_id, start_date, end_date)` returns `total_units`.
  - `get_item_history(item_id, store_id, start_date, end_date)` returns observed `unit_sales`.
  - `get_eligible_items(store_id, search_text, limit=50)` returns forecast-eligible item/store pairs, not historical sales.
- Approved stores come from `context.coverage.available_stores`.
- Repository defaults permit 366 inclusive history days and 1,000 rows. Application Design §12.1 specifies at most 365 daily chart points; use the stricter presentation bound.
- Item history validates dates, duplicates, and numerical values but permits gaps. Store trends lack equivalent daily-grain guards. Presentation must reject incomplete windows before displaying sales results.
- `PublicationCoverage` exposes series counts, aggregate serving-row counts, and stores—not historical dates, historical day counts, or distinct-product counts.
- Publisher validation enforces the frozen 194,100-observation population and reconciles store aggregates with item sales. These checks do not expose history coverage through the application context.
- `coverage.loaded_rows` counts rows across serving datasets; it is not an observed-sales count or a historical-day count.
- `pyproject.toml` declares Streamlit; `uv.lock` resolves 1.66.1. Plotly is not declared or locked.
- Existing shell tests use Streamlit `AppTest`, synthetic contexts, and mocked runtime I/O. Their repository mocks and placeholder assumptions require adaptation.
- Relevant specification: Application Design §§4, 7, 11–14; Requirements **FR-14**.
- Discovery covered Overview presentation and its direct state, repository, publication-evidence, dependency, and test dependencies.

# Proposed Changes

### 1. Establish trustworthy coverage inputs
- Resolve the missing loaded-history metadata through the owning upstream task; do not add read contracts or schema in Task 03.
- Required evidence: publication/source-bound historical bounds and measured loaded-day count, with store applicability explicit.
- Do not scan successive history windows to discover coverage.
- Derive approved store count from the distinct approved store catalog.
- Derive supported-product count from the complete bounded eligible catalog for the selected store. Label that scope explicitly; never use series count as distinct-product count.
- A catalog failure makes product coverage unavailable, not zero. An empty valid catalog means no supported products.
- Display original-M5 population only as clearly labeled context, separate from measured loaded M5 Small coverage.

### 2. Implement validated selection behavior
- Offer one approved store at a time; initialize an unset selection deterministically from the approved catalog.
- Use shared `set_selection` for store/product changes, with separate widget keys to avoid mutating instantiated widgets.
- Read the bounded eligible catalog for the selected store. Offer **“Tất cả sản phẩm”** as the unfiltered product option.
- Explain that product choices are supported eligible products; catalog provenance must not make historical sales appear predicted.
- Validate existing selections against the current publication/catalog. Report unsupported values rather than silently replacing them with another product.
- Bound date controls to confirmed loaded history. Require typed, ordered dates and an inclusive span no greater than `min(365, repository.limits.max_history_days, repository.limits.max_rows)`.
- Default to the latest permitted window within confirmed coverage.
- Apply date changes together through **“Áp dụng”**. Incomplete or invalid dates trigger feedback without a history read or retained sales output.
- Keep page-owned date/widget state under `m09.*` so existing publication invalidation clears it.

### 3. Build one reconciled sales presentation
- For all products, call only `get_store_trend`; for a selected product, call only `get_item_history`.
- Normalize the returned bounded rows into one chronological presentation dataset without SQL or additional fact reads.
- Require `state == 'ok'`, observed provenance, valid selected identities, unique dates inside the requested interval, and finite nonnegative quantities.
- Require exactly one observation for every selected date. Preserve explicit zero sales; reject missing dates without filling or partial plotting.
- Compute filtered units from this validated dataset once; use the identical rows for chart and table.
- Keep coverage KPIs in a separate section so changing the selected window does not redefine loaded-history coverage.
- Block sales output on malformed, incomplete, unavailable, or erroneous reads. Never retain a previous successful chart under new filters.

### 4. Present Vietnamese meaning and provenance
- Render **“Tổng quan”**, the required M5 historical-data notice, and an explicit observed-unit-sales explanation.
- Show approved publication ID, source snapshot, scope identity where available, selected store/product, and displayed dates under **“Nguồn và phạm vi dữ liệu”**.
- Render a Plotly daily line chart with Vietnamese title, axes, legend, and hover labels; dates use `DD/MM/YYYY`.
- Render an accessible detail table with Vietnamese headers and the selected store/product context.
- Format displayed counts with Vietnamese separators while preserving numerical values for reconciliation.
- Reuse `render_read_state` for failures; provide specific Vietnamese unsupported-filter and invalid-date feedback.
- Keep engineering diagnostics sanitized and in English. Avoid new result caching; retain live render-scoped reads.

# API Surface

- Preserve `render_overview(context, repository, selections)` as the shell entry point.
- Delegate Overview rendering to `app/overview.py`; leave the other destination entry points intact.
- Add small internal helpers for filter validation, daily-row normalization, and Plotly construction in that module.
- Keep helpers deterministic where practical so integrity and reconciliation are directly testable.
- Consume existing context, selection helpers, repository methods, limits, and `ReadResult`.
- No new repository methods, approval validators, SQL, or public data contracts.
- **Unresolved prerequisite:** identify the approved upstream carrier for measured history coverage before BUILD; its implementation is outside this task.

# Configuration

- Add Plotly as a direct project dependency and update `uv.lock` using `uv`.
- Select a compatible Plotly release and verify the supported chart call against locked Streamlit.
- Keep existing application configuration, scoped client ownership, and repository limits.
- No new environment variables, services, or cache configuration.

# File Delta

**Modify**
- `app/views.py`
- `tests/unit/test_application_shell.py`
- `pyproject.toml`
- `uv.lock`

**Add**
- `app/overview.py`
- `tests/unit/test_application_overview.py`

**Remove**
- None.

# Focused Tests

- Write focused synthetic presentation tests before production changes.
- Verify store-wide versus item-specific read routing and exact filter arguments.
- Verify store changes clear product selection, invalid products are rejected, and publication/checksum changes invalidate Overview state.
- Cover loaded-history boundaries, inclusive span limits, reversed dates, unfinished date selection, and invalid filters causing no history read.
- Verify coverage metrics use their declared sources and scopes, independently of filtered sales.
- Use unordered daily rows with explicit zeros; assert chronological chart/table rows and exact KPI reconciliation.
- Reject missing dates, duplicates, out-of-range dates, invalid quantities, wrong provenance, and mismatched identities.
- Cover every `ReadResult` state, missing detail binding, catalog failures, and success-to-failure transitions without stale sales output.
- Check Vietnamese labels, historical truth notices, source identity, and separation of original versus loaded population.
- Update shell fixtures with explicit bounded read results; retain approval, navigation, cleanup, and sanitized-failure assertions.
- Focused command after implementation:
  `uv run --frozen pytest tests/unit/test_application_overview.py tests/unit/test_application_shell.py tests/unit/test_application_session_state.py tests/unit/test_application_read_repository.py`
- No tests were executed during this read-only planning phase.

# Out of Scope

- Forecast presentation, inventory, replenishment, allocation, policy evaluation, downloads, and what-if controls.
- New read contracts/schema, publisher changes, approval duplication, and catalog SQL.
- All-store queries, full-M5 scans, ETL, retraining, upstream artifact regeneration, and service startup.
- Implementation remains blocked on an approved source of measured loaded-history coverage; do not substitute nominal population constants.
