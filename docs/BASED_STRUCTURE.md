**This folder structure is a planned reference layout, not an inventory of the
currently tracked repository.** The directories below should be created only when
their first real implementation requires them. Generated data and runtime
artifacts remain local and are not source-controlled.

warehouse-bigdata
│
├── .github/
│   └── workflows/
│
├── docs/
│
├── infrastructure/
│   ├── hadoop/
│   │   └── config/
│   ├── spark/
│   │   └── config/
│   └── clickhouse/
│       └── config/
│
├── src/
│   └── warehouse_bigdata/
│       ├── common/
│       ├── ingestion/
│       ├── processing/
│       ├── forecasting/
│       ├── optimization/
│       ├── publication/
│       └── jobs/
│
├── app/
│   ├── pages/
│   ├── components/
│   └── data_access/
│
├── sql/
│   ├── migrations/
│   └── queries/
│
├── tests/
│   ├── fixtures/
│   ├── unit/
│   ├── data/
│   ├── integration/
│   ├── application/
│   ├── performance/
│   └── manifests/
│
├── scripts/
│
├── data/
│   ├── downloads/
│   └── samples/
│
└── artifacts/
    ├── models/
    ├── manifests/
    ├── reports/
    └── test-runs/

**Suggested future folder structure; not all directories currently exist or contain
tracked files.**
