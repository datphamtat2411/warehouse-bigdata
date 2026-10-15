# FND-06 Fixtures

These fixtures are small, deterministic synthetic records. They are not genuine
M5 observations and do not represent real inventory, demand, model accuracy, or
policy performance. Every synthetic identity is prefixed with
`synthetic-fnd06-`; item and store labels remain the small explicit values
`ITEM_A`, `ITEM_B`, `CA_1`, and `CA_2`.

`fnd06_records.json` contains one valid record set for each cataloged dataset,
one manifest for each cataloged manifest contract, and one publication context.
`fnd06_expected.json` contains independently written counts, totals, dates,
horizons, lineage identities, and publication metadata used by the tests. The
fixture loader only reads JSON and does not calculate or repair values.

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
