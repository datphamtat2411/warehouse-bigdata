# Contract Fixtures

These fixtures are small, deterministic synthetic records. They are not genuine
M5 observations and do not represent real inventory, demand, model accuracy, or
policy performance. Every synthetic artifact identity is prefixed with
`synthetic-`; item and store labels remain the small explicit values `ITEM_A`,
`ITEM_B`, `CA_1`, and `CA_2`.

## Structure

`context.json` owns synthetic provenance and shared lineage identities.
`datasets/sales.json` owns source, enrichment, and store aggregate records.
`datasets/forecasting.json` owns features, forecasts, and forecast metrics.
`datasets/inventory.json` owns simulated inventory, supply, decision, allocation,
and evaluation records. `manifests.json` owns the eight manifest categories.
`publication.json` owns the publication context and application metadata copy.
`expected.json` contains independently authored golden counts, totals, dates,
horizons, lineage identities, and publication metadata.

The loader reads JSON only when a function is called and returns a fresh mutable
object each time:

```python
from tests.fixtures.loader import load_dataset, load_expected

sales = load_dataset("sales_daily")
golden = load_expected("sales_daily")
```

Use `load_context()`, `load_manifest(name)`, and `load_publication_fixture()` for
the other fixture groups. Unknown resource names are rejected with `KeyError`.

Golden expected values are never calculated by the validator or loader. Tests
use them to check counts, numeric totals, seven-day coverage, and identity
relationships independently of validation implementation.

## Contract Coverage

| Contract area | Fixture case | Test IDs |
|---|---|---|
| Source and enrichment | Observed zero/positive sales and one nullable price | FND06-DATA-001 through FND06-DATA-003 |
| Aggregates and features | Four store/date totals and one typed feature row | FND06-DATA-004, FND06-FEAT-001 |
| Forecasts and metrics | Two seven-day series and nullable validation metadata | FND06-FC-001 through FND06-FC-005 |
| Inventory and decisions | Simulated stock, central supply, replenishment, allocation, and historical evaluation | FND06-INV-001 through FND06-INV-004 |
| Run manifests | Eight cataloged manifest shapes and lineage identities | FND06-MAN-001 through FND06-MAN-007 |
| Publication | Approved manifest, context, checksum, and application copy | FND06-PUB-001 and FND06-PUB-002 |

Calendar and weekly-price lookup cases remain deferred because FND-05 does not
define contracts for those interfaces. These tests validate contract behavior
without Hadoop, Spark, HDFS, ClickHouse, Docker, network access, credentials,
or model and policy implementations.

## Test Conventions And Limits

Valid cases preserve the approved Contract v1 shapes, including zero sales,
nullable prices, seven-day forecasts, simulated inventory provenance, and
publication approval metadata. Invalid cases copy a valid record and mutate one
field so the expected diagnostic code remains explicit.

Fixture `sha256:` values and `size_bytes` values are deterministic placeholders.
The tests check presence and cross-artifact compatibility only; they do not claim
cryptographic artifact verification or measured file sizes. The fixture bundle
also preserves a known partial forecast-to-decision relationship because
Contract v1 does not define a cross-dataset foreign-key rule.

The Python validator is intended for small bounded contract fixtures. Each
`validate_dataset()` call accepts at most 10,000 records and consumes at most one
additional record to detect an oversized iterable. Memory still scales with
records, unique keys, and grouped validation state, so this is not constant
memory. It is not suitable for full M5-scale processing; distributed Spark
data-quality operations own that workload.

Fixture loading and validation are deterministic and require no external
services, network access, or random state.
