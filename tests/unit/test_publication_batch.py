"""Bounded publication, physical reconciliation, and fail-closed lifecycle tests."""

import hashlib
from copy import deepcopy

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tests.fixtures.loader import load_dataset
from warehouse_bigdata.contracts.datasets import SERVING_DATASET_MAPPINGS
from warehouse_bigdata.publication.batch import (
    PublicationDataset,
    PublicationError,
    PublicationSelection,
    ReconciliationReport,
    map_batch_to_serving_rows,
    publish_offline_reload,
    reconcile_publication,
    validate_dataset_bindings,
    validate_offline_gate,
)
from warehouse_bigdata.publication.parquet import ParquetFile, iter_parquet_batches


@pytest.mark.parametrize("mapping", SERVING_DATASET_MAPPINGS, ids=lambda m: m.serving_dataset)
def test_all_contract_mappings_preserve_order_and_omissions(mapping):
    source = load_dataset(mapping.logical_dataset)
    columns, rows = map_batch_to_serving_rows(
        mapping.logical_dataset, source, lineage={"source_snapshot_id": "synthetic-snapshot-001"}
    )
    assert len(rows) == len(source)
    assert len(set(columns)) == len(columns)
    assert "raw_prediction" not in columns
    assert not set(mapping.omitted_fields) & set(columns)
    for field in mapping.field_mappings:
        if field.serving_field:
            assert field.serving_field in columns
    if mapping.serving_dataset == "sales_daily":
        assert "sale_date" in columns and "date" not in columns
        assert rows[0][columns.index("source_snapshot_id")] == "synthetic-snapshot-001"
        assert rows[0][columns.index("unit_sales")] == source[0]["unit_sales"]
    if mapping.serving_dataset == "forecast_daily":
        assert {row[columns.index("forecast_status")] for row in rows} == {"ok"}


@pytest.mark.parametrize("value", [-1, 2**32, 1.25, True, float("nan"), None])
def test_mapping_rejects_lossy_sales_casts(value):
    rows = deepcopy(load_dataset("sales_daily"))
    rows[0]["unit_sales"] = value
    with pytest.raises((PublicationError, ValueError)):
        map_batch_to_serving_rows("sales_daily", rows, lineage={"source_snapshot_id": "s"})


def test_mapping_preserves_nullable_metrics_and_allocation_components():
    columns, rows = map_batch_to_serving_rows(
        "forecast_metrics", load_dataset("forecast_metrics"), lineage={}
    )
    assert rows[0][columns.index("metric_value")] is None
    source = deepcopy(load_dataset("allocation_recommendations"))
    source[0]["critical_allocated_units"] = None
    source[0]["safety_allocated_units"] = None
    columns, rows = map_batch_to_serving_rows("allocation_recommendations", source, lineage={})
    assert rows[0][columns.index("critical_allocated_units")] is None
    assert rows[0][columns.index("safety_allocated_units")] is None


def test_timestamp_mapping_explicitly_converts_to_utc_milliseconds():
    rows = deepcopy(load_dataset("forecast_daily"))
    rows[0]["created_at"] = "2026-01-07T00:00:00.123456Z"
    columns, mapped = map_batch_to_serving_rows("forecast_daily", rows, lineage={})
    assert mapped[0][columns.index("created_at")].microsecond == 123000


def test_preflight_rejects_unresolved_selector_without_io():
    from warehouse_bigdata.contracts.dataset_scope import ScopeError
    from warehouse_bigdata.publication.batch import preflight_publication_inputs

    selector = dict(
        schema_version="small-artifacts-v1",
        dataset_scope="demo_sample",
        source_version="sha256:" + "a" * 64,
        source_snapshot_id="small",
        selection_digest=None,
        feature_version="v1",
        model_run_id="m",
        forecast_run_id="f",
        validation_evaluation_run_id="ve",
        test_evaluation_run_id="te",
        **{
            stage + "_pipeline_run_id": stage
            for stage in ("j02", "j03", "j04", "j05", "j06", "validation", "test")
        },
    )
    with pytest.raises(ScopeError, match="selection_digest"):
        preflight_publication_inputs(
            selector,
            decision_run_id="d",
            decision_pipeline_run_id="dp",
            policy_evaluation_run_id="e",
            policy_evaluation_pipeline_run_id="ep",
            forecast_evaluation_run_id="te",
            client=None,
            config=None,
            spark=None,
        )


class LocalHdfs:
    def __init__(self, payload):
        self.payload = payload
        self.downloads = []

    def download_file(self, remote, local, *, max_bytes):
        assert len(self.payload) <= max_bytes
        self.downloads.append(remote)
        local.write_bytes(self.payload)


def test_reader_batches_one_file_and_cleans_scratch(tmp_path):
    source = tmp_path / "source.parquet"
    pq.write_table(pa.table({"x": list(range(13))}), source)
    payload = source.read_bytes()
    client = LocalHdfs(payload)
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    descriptor = ParquetFile(
        "/selected/part-0.parquet", len(payload), "sha256:" + hashlib.sha256(payload).hexdigest()
    )
    iterator = iter_parquet_batches(client, [descriptor], scratch_dir=scratch, batch_rows=4)
    assert [batch.num_rows for batch in iterator] == [4, 4, 4, 1]
    assert client.downloads == [descriptor.path]
    assert list(scratch.iterdir()) == []


def test_reader_cleans_on_early_close_and_rejects_corruption(tmp_path):
    source = tmp_path / "source.parquet"
    pq.write_table(pa.table({"x": list(range(12))}), source)
    payload = source.read_bytes()
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    descriptor = ParquetFile("/selected/part-0.parquet", len(payload))
    iterator = iter_parquet_batches(
        LocalHdfs(payload), [descriptor], scratch_dir=scratch, batch_rows=2
    )
    next(iterator)
    iterator.close()
    assert list(scratch.iterdir()) == []
    with pytest.raises(ValueError):
        list(
            iter_parquet_batches(
                LocalHdfs(b"bad"),
                [ParquetFile(descriptor.path, 3)],
                scratch_dir=scratch,
                batch_rows=2,
            )
        )
    assert list(scratch.iterdir()) == []


@pytest.mark.parametrize("batch_rows", [0, -1, 10001])
def test_reader_rejects_unbounded_batches(tmp_path, batch_rows):
    with pytest.raises(ValueError):
        list(iter_parquet_batches(LocalHdfs(b""), [], scratch_dir=tmp_path, batch_rows=batch_rows))


def _bindings():
    common = dict(
        source_snapshot_id="s",
        selection_digest="sha256:" + "a" * 64,
        dataset_scope="demo_sample",
        model_run_id="m",
    )
    return {
        "forecast_daily": dict(
            common, forecast_run_id="forward", issue_date="2016-05-22", run_type="forward_demo"
        ),
        "forecast_metrics": dict(
            common,
            evaluation_run_id="forecast-eval",
            run_type="test",
            holdout_id="holdout",
            origin_forecasts={"1913": "historic"},
        ),
        "inventory_policy_evaluation": dict(
            common,
            evaluation_run_id="policy-eval",
            forecast_run_id="historic",
            issue_date="2016-04-24",
            scenario_id="historic-scenario",
            run_type="test",
            decision_run_ids={"proposed": "historic-proposed", "baseline": "historic-baseline"},
            policy_ids=["proposed", "baseline"],
            policy_version="v1",
            label_provenance="observed_m5_unit_sales_demand_proxy",
        ),
    }


def test_historical_bindings_do_not_require_forward_demo_identity_equality():
    bindings = _bindings()
    validate_dataset_bindings(bindings)
    assert (
        bindings["inventory_policy_evaluation"]["forecast_run_id"]
        != bindings["forecast_daily"]["forecast_run_id"]
    )


@pytest.mark.parametrize(
    "field,value",
    [
        ("selection_digest", "other"),
        ("source_snapshot_id", "other"),
        ("run_type", "forward_demo"),
        ("label_provenance", None),
    ],
)
def test_historical_binding_rejects_incompatible_scope_and_missing_labels(field, value):
    bindings = _bindings()
    bindings["inventory_policy_evaluation"][field] = value
    with pytest.raises(PublicationError):
        validate_dataset_bindings(bindings)


@pytest.mark.parametrize("offline,confirm", [(False, False), (True, False), (False, True)])
def test_offline_reload_requires_both_explicit_gates(offline, confirm):
    with pytest.raises(PublicationError):
        validate_offline_gate(offline, confirm)


class MetadataHdfs:
    def __init__(self):
        self.files = {}
        self.events = []

    def mkdir_parents(self, path):
        pass

    def mkdir_exclusive(self, path):
        if path in self.files:
            raise RuntimeError("concurrent publication")
        self.files[path] = b""

    def exists(self, path):
        return path in self.files

    def upload_bytes(self, payload, path):
        assert path not in self.files
        self.files[path] = payload
        self.events.append(path)

    def read_remote(self, path):
        return self.files[path]

    def rename_replace(self, source, target):
        self.files[target] = self.files.pop(source)

    def remove(self, path):
        del self.files[path]


def test_failed_reload_keeps_unavailable_and_does_not_change_approval(tmp_path, monkeypatch):
    from warehouse_bigdata.publication import batch

    client = MetadataHdfs()
    pointer = "/data/warehouse/manifests/published/current.json"
    client.files[pointer] = b"old approval"
    copy = tmp_path / "approved_publication.json"
    copy.write_bytes(b"old approval")
    selection = PublicationSelection({}, {}, (), {})
    monkeypatch.setattr(batch, "validate_source_data", lambda *args, **kwargs: {})

    class BrokenServing:
        def prepare(self):
            pass

        def clear(self):
            raise RuntimeError("second shard unavailable")

    with pytest.raises(RuntimeError, match="second shard"):
        publish_offline_reload(
            selection,
            client=client,
            serving=BrokenServing(),
            scratch_dir=tmp_path,
            application_path=copy,
            offline_reload=True,
            confirm_offline_reload=True,
        )
    assert client.files[pointer] == b"old approval"
    assert copy.read_bytes() == b"old approval"
    assert client.exists("/data/warehouse/manifests/published/unavailable.json")
    assert any(path.endswith(".failed.json") for path in client.events)


def test_reconciliation_requires_shard_and_distributed_equality():
    class Serving:
        def inspect(self, dataset, expected):
            return {
                "distributed": expected,
                "shards": {1: expected, 2: expected},
                "routing_errors": 0,
            }

        def validate_relations(self):
            return {}

    dataset = PublicationDataset("sales_daily", "/selected", (), 1, {}, {})
    selection = PublicationSelection({}, {}, (dataset,), {})
    stats = {"sales_daily": {"rows": 1, "measures": {}, "nulls": {}, "fingerprint": [1, 2]}}
    with pytest.raises(PublicationError, match="shard"):
        reconcile_publication(selection, stats, serving=Serving())


def test_repeated_publication_replaces_instead_of_appending_and_copies_exactly(
    tmp_path, monkeypatch
):
    import json

    from warehouse_bigdata.publication import batch

    bindings = _bindings()
    forward = dict(
        bindings["forecast_daily"],
        scenario_id="forward-scenario",
        decision_run_id="forward-decision",
        policy_id="proposed",
        policy_version="v1",
    )
    datasets = tuple(
        PublicationDataset(
            m.logical_dataset,
            "/selected/" + m.logical_dataset,
            (),
            len(load_dataset(m.logical_dataset)),
            {},
            bindings.get(m.logical_dataset, forward),
        )
        for m in SERVING_DATASET_MAPPINGS
    )
    context = dict(
        source_snapshot_id="s",
        model_run_id="m",
        forecast_run_id="forward",
        scenario_id="forward-scenario",
        decision_run_id="forward-decision",
        issue_date="2016-05-22",
        policy_ids=["proposed"],
        evaluation_run_id=None,
        holdout_id=None,
    )
    selection = PublicationSelection(
        {"source_snapshot_id": "s"},
        {
            "selected_series": [
                {"item_id": "ITEM_A", "store_id": "CA_1"},
                {"item_id": "ITEM_A", "store_id": "CA_2"},
            ]
        },
        datasets,
        context,
    )

    def batches(dataset, *args):
        records = load_dataset(dataset.name)
        yield (
            records,
            map_batch_to_serving_rows(
                dataset.name,
                records,
                lineage={"source_snapshot_id": "synthetic-snapshot-001"}
                if dataset.name == "sales_daily"
                else {},
            ),
        )

    stats = {}
    for dataset in datasets:
        stats[dataset.name] = batch._new_stats(dataset.name)
        _, (_, rows) = next(batches(dataset))
        batch._add_stats(stats[dataset.name], dataset.name, rows)
    monkeypatch.setattr(batch, "_iterate_dataset", batches)
    monkeypatch.setattr(batch, "validate_source_data", lambda *args, **kwargs: stats)

    class Serving:
        def __init__(self):
            self.clears = 0
            self.rows = {}
            self.fail_once = True

        def prepare(self):
            pass

        def clear(self):
            self.clears += 1
            self.rows = {d.name: [] for d in datasets}

        def insert(self, dataset, columns, rows):
            self.rows[dataset.name].extend(rows)
            if self.fail_once:
                self.fail_once = False
                raise RuntimeError("partial insert")

        def inspect(self, dataset, expected):
            total = batch._new_stats(dataset.name)
            batch._add_stats(total, dataset.name, self.rows[dataset.name])
            shards = {}
            for shard in (1, 2):
                shards[shard] = batch._new_stats(dataset.name)
                batch._add_stats(
                    shards[shard], dataset.name, self.rows[dataset.name][shard - 1 :: 2]
                )
            return dict(distributed=total, shards=shards, routing_errors=0)

        def validate_relations(self):
            return {}

    hdfs, serving = MetadataHdfs(), Serving()
    copy = tmp_path / "approved_publication.json"
    results = []
    with pytest.raises(RuntimeError, match="partial insert"):
        publish_offline_reload(
            selection,
            client=hdfs,
            serving=serving,
            scratch_dir=tmp_path,
            application_path=copy,
            offline_reload=True,
            confirm_offline_reload=True,
        )
    assert not copy.exists()
    assert not hdfs.exists("/data/warehouse/manifests/published/current.json")
    assert hdfs.exists("/data/warehouse/manifests/published/unavailable.json")
    for _ in range(2):
        result = publish_offline_reload(
            selection,
            client=hdfs,
            serving=serving,
            scratch_dir=tmp_path,
            application_path=copy,
            offline_reload=True,
            confirm_offline_reload=True,
        )
        results.append(result)
        assert result.status == "APPROVED"
        assert all(len(serving.rows[d.name]) == d.expected_rows for d in datasets)
        payload = hdfs.files["/data/warehouse/manifests/published/current.json"]
        assert payload == copy.read_bytes()
        approved = json.loads(hdfs.files[result.manifest_path])
        assert (
            approved["application_manifest_checksum"]
            == "sha256:" + hashlib.sha256(payload).hexdigest()
        )
        assert not hdfs.exists("/data/warehouse/manifests/published/unavailable.json")
        assert (
            json.loads(payload)["dataset_bindings"]["inventory_policy_evaluation"][
                "forecast_run_id"
            ]
            == "historic"
        )
    assert serving.clears == 3
    assert results[0].publication_id != results[1].publication_id


def test_reader_rejects_changed_checksums_and_oversized_files(tmp_path):
    class Client:
        def checksum(self, path):
            return "changed"

        def download_file(self, *args, **kwargs):
            pytest.fail("must reject before download")

    with pytest.raises(ValueError, match="checksum"):
        list(
            iter_parquet_batches(
                Client(),
                [ParquetFile("/selected/part-0.parquet", 10, checksum="sealed")],
                scratch_dir=tmp_path,
            )
        )
    with pytest.raises(ValueError, match="oversized"):
        list(
            iter_parquet_batches(
                Client(), [ParquetFile("/selected/part-0.parquet", 2**40)], scratch_dir=tmp_path
            )
        )


@pytest.mark.parametrize("problem", ["distributed", "routing", "shard", "measure"])
def test_reconciliation_rejects_partial_results_wrong_routes_and_changed_measures(problem):
    from warehouse_bigdata.publication import batch

    expected = batch._new_stats("sales_daily")
    expected["rows"] = 2
    shard = deepcopy(expected)
    shard["rows"] = 1
    report = dict(
        distributed=deepcopy(expected),
        shards={1: deepcopy(shard), 2: deepcopy(shard)},
        routing_errors=0,
    )
    if problem == "distributed":
        report["distributed"]["rows"] = 1
    elif problem == "routing":
        report["routing_errors"] = 1
    elif problem == "shard":
        del report["shards"][2]
    else:
        report["distributed"]["measures"]["unit_sales"] = 1

    class Serving:
        def inspect(self, *args):
            return report

        def validate_relations(self):
            pytest.fail("cannot check relations after failed equality gate")

    selection = PublicationSelection(
        {}, {}, (PublicationDataset("sales_daily", "/selected", (), 2, {}, {}),), {}
    )
    with pytest.raises(PublicationError):
        reconcile_publication(selection, {"sales_daily": expected}, serving=Serving())


@pytest.fixture
def selected_preflight(monkeypatch):
    import json
    from types import SimpleNamespace

    from warehouse_bigdata.common import scope_evidence
    from warehouse_bigdata.common.hdfs_paths import (
        render_curated_sales_daily_path,
        render_curated_store_daily_sales_path,
        render_decision_output_path,
        render_evaluation_output_path,
        render_forecast_output_path,
    )
    from warehouse_bigdata.optimization import inventory_artifacts
    from warehouse_bigdata.publication import batch

    selector = dict(
        schema_version="small-artifacts-v1",
        dataset_scope="demo_sample",
        source_version="sha256:" + "a" * 64,
        source_snapshot_id="small",
        selection_digest="sha256:" + "b" * 64,
        feature_version="v1",
        model_run_id="m",
        forecast_run_id="forward",
        validation_evaluation_run_id="ve",
        test_evaluation_run_id="te",
        **{
            stage + "_pipeline_run_id": stage
            for stage in ("j02", "j03", "j04", "j05", "j06", "validation", "test")
        },
    )
    config = SimpleNamespace(base_path="/data/warehouse", uri="hdfs://namenode:9000")
    scope = dict(
        dataset_scope="demo_sample",
        selection_digest=selector["selection_digest"],
        selected_series=[dict(item_id="item", store_id="store")],
    )
    documents = {}
    files = {}

    def inventory(root, names):
        records = []
        for name in names:
            prefix = name + "/" if name else ""
            records.append(dict(path=prefix + "part-0.parquet", size_bytes=4, checksum="sealed"))
            files[root + ("/" + name if name else "")] = [
                dict(path=root + "/" + prefix + "part-0.parquet", type="FILE", length=4)
            ]
        return records

    for _name, renderer, marker, count in (
        ("sales_daily", render_curated_sales_daily_path, "_snapshot_manifest.json", 194100),
        (
            "store_daily_sales",
            render_curated_store_daily_sales_path,
            "_dataset_manifest.json",
            19410,
        ),
    ):
        path = renderer(config.base_path, "small")
        documents[path + "/" + marker] = dict(status="VALIDATED", row_count=count)
        inventory(path, [""])
    path = render_forecast_output_path(config.base_path, "forward")
    documents[path + "/scoring_manifest.json"] = dict(
        status="VALIDATED",
        row_count=700,
        forecast_run_id="forward",
        model_run_id="m",
        source_snapshot_id="small",
        feature_version="v1",
        run_type="forward_demo",
        issue_date="2016-05-22",
    )
    documents[path + "/evidence/publication.json"] = dict(
        file_integrity=inventory(path, ["forecast_daily"])
    )
    for kind, root, run, identity, names in (
        (
            "decision",
            render_decision_output_path(config.base_path, "d"),
            "dp",
            "d",
            [
                "inventory_scenario",
                "central_supply_scenario",
                "replenishment_recommendations",
                "allocation_recommendations",
            ],
        ),
        (
            "evaluation",
            render_evaluation_output_path(config.base_path, "pe"),
            "pep",
            "pe",
            ["inventory_policy_evaluation"],
        ),
    ):
        local = dict(
            scope=deepcopy(scope),
            pipeline_run_id=run,
            source_snapshot_id="small",
            model_run_id="m",
            scenario_id=kind + "-scenario",
            policy_version="v1",
            upstream_identities=deepcopy(selector),
            forecast_run_id="forward" if kind == "decision" else "historical",
            issue_date="2016-05-22" if kind == "decision" else "2016-04-24",
            run_type="forward_demo" if kind == "decision" else "test",
        )
        local["decision_run_id" if kind == "decision" else "evaluation_run_id"] = identity
        if kind == "decision":
            local["policy_id"] = "proposed"
        documents[root + "/evidence/decision_binding.json"] = local
        documents[
            root
            + "/"
            + ("decision_manifest.json" if kind == "decision" else "evaluation_manifest.json")
        ] = dict(status="VALIDATED", holdout_id="holdout")
        documents[root + "/evidence/publication.json"] = dict(
            file_integrity=inventory(root, names), datasets={n: dict(row_count=100) for n in names}
        )
        if kind == "evaluation":
            documents[root + "/evidence/audit.json"] = dict(
                metadata=dict(
                    decision_run_ids={
                        "proposed": "historical-decision",
                        "baseline": "historical-baseline",
                    },
                    label_provenance="observed_m5_unit_sales_demand_proxy",
                ),
                frozen=dict(policy_version="v1"),
            )
    path = render_evaluation_output_path(config.base_path, "te")
    documents[path + "/evaluation_manifest.json"] = dict(
        status="VALIDATED",
        row_count=48,
        evaluation_run_id="te",
        source_snapshot_id="small",
        model_run_id="m",
        holdout_id="holdout",
        run_type="test",
        evaluation_start="2016-04-25",
        evaluation_end="2016-05-22",
    )
    documents[path + "/evidence/evaluation.json"] = dict(
        origin_forecasts={"1913": "historical"}, pipeline_run_id="test"
    )
    documents[path + "/evidence/publication.json"] = dict(
        file_integrity=inventory(path, ["metrics"])
    )

    class Client:
        def read_remote(self, path):
            assert path in documents, "unexpected identity/discovery path: " + path
            return json.dumps(documents[path]).encode()

        def list_status(self, path):
            assert path in files, "unexpected discovery path: " + path
            return files[path]

        def checksum(self, path):
            return "sealed"

    monkeypatch.setattr(batch, "validate_small_publications", lambda *args, **kwargs: None)
    monkeypatch.setattr(batch, "require_sealed", lambda *args: None)
    monkeypatch.setattr(scope_evidence, "read_scope", lambda *args, **kwargs: scope)
    monkeypatch.setattr(
        inventory_artifacts,
        "verify_inventory_bundle",
        lambda spark, client, config, path: documents[
            path
            + "/"
            + ("decision_manifest.json" if "/decisions/" in path else "evaluation_manifest.json")
        ],
    )

    def preflight():
        return batch.preflight_publication_inputs(
            selector,
            decision_run_id="d",
            decision_pipeline_run_id="dp",
            policy_evaluation_run_id="pe",
            policy_evaluation_pipeline_run_id="pep",
            forecast_evaluation_run_id="te",
            client=Client(),
            config=config,
            spark=None,
        )

    return preflight, documents


def test_preflight_preserves_independent_historical_and_primary_identities(selected_preflight):
    preflight, _ = selected_preflight
    result = preflight()
    assert len(result.datasets) == 9
    assert result.context["forecast_run_id"] == "forward"
    assert result.context["evaluation_run_id"] is None
    assert result.bindings["inventory_policy_evaluation"]["forecast_run_id"] == "historical"
    assert result.bindings["forecast_metrics"]["origin_forecasts"] == {"1913": "historical"}


@pytest.mark.parametrize(
    "mutation", ["status", "scope", "upstream", "forward_eval", "labels", "checksum"]
)
def test_preflight_rejects_invalid_selected_artifacts(selected_preflight, mutation):
    preflight, documents = selected_preflight
    root = "/data/warehouse/outputs/evaluation/pe"
    binding = documents[root + "/evidence/decision_binding.json"]
    if mutation == "status":
        documents[root + "/evaluation_manifest.json"]["status"] = "COMPLETED"
    elif mutation == "scope":
        binding["scope"]["selection_digest"] = "other"
    elif mutation == "upstream":
        binding["upstream_identities"]["test_evaluation_run_id"] = "other"
    elif mutation == "forward_eval":
        binding["run_type"] = "forward_demo"
    elif mutation == "labels":
        documents[root + "/evidence/audit.json"]["metadata"]["label_provenance"] = None
    else:
        documents[root + "/evidence/publication.json"]["file_integrity"][0]["checksum"] = "changed"
    with pytest.raises(PublicationError):
        preflight()


def test_approval_cannot_precede_validated_reconciliation(tmp_path):
    from warehouse_bigdata.publication.batch import approve_publication

    with pytest.raises(PublicationError, match="VALIDATED"):
        approve_publication(
            PublicationSelection({}, {}, (), {}),
            {"status": "RUNNING"},
            ReconciliationReport(False),
            client=None,
            application_path=tmp_path / "approved.json",
        )


def test_source_uniqueness_spills_across_batches_and_cleans_on_failure(tmp_path, monkeypatch):
    from warehouse_bigdata.publication import batch

    row = load_dataset("sales_daily")[0]
    dataset = PublicationDataset("sales_daily", "/selected", (), 2, {"status": "VALIDATED"}, {})
    selection = PublicationSelection(
        {},
        {"selected_series": [{"item_id": row["item_id"], "store_id": row["store_id"]}]},
        (dataset,),
        {},
    )
    # Keep the synthetic fixture tiny, and hit cross-batch duplicate validation
    # before the full frozen-population acceptance gate.
    row = dict(row, day_index=1, date="2011-01-29")

    def batches(*args):
        for _ in range(2):
            yield (
                [row],
                map_batch_to_serving_rows(
                    "sales_daily", [row], lineage={"source_snapshot_id": "s"}
                ),
            )

    monkeypatch.setattr(batch, "_iterate_dataset", batches)
    with pytest.raises(PublicationError, match="duplicate source"):
        batch.validate_source_data(selection, client=None, scratch_dir=tmp_path, batch_rows=1)
    assert list(tmp_path.iterdir()) == []
