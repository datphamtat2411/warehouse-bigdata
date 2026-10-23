"""Bounded J07 Contract v1 bundles, independent readback and owner publication.

Only completion/publication documents are excluded from the file inventory; their
digests are bound by the immutable request. Audit JSON is not a serving dataset.
"""

from collections import Counter
from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
import json
from pathlib import PurePosixPath

from warehouse_bigdata.common.hdfs_namespace import (
    HdfsNamespaceError, require_owner, validate_publication_namespace,
)
from warehouse_bigdata.common.hdfs_paths import (
    render_decision_output_path, render_evaluation_output_path, render_staging_path,
)
from warehouse_bigdata.common.hdfs_publication import (
    _bytes, _envelope, _freeze_feature_metadata, _json, _require_private, _seal, document_digest,
)
from warehouse_bigdata.common.scope_evidence import (
    bind_scope, read_scope, validate_scope_binding, write_scope,
)
from warehouse_bigdata.common.small_artifacts import validate_small_artifacts
from warehouse_bigdata.contracts.datasets import INVENTORY_POLICY_EVALUATION
from warehouse_bigdata.contracts.manifests import (
    DECISION_MANIFEST, EVALUATION_MANIFEST, SCENARIO_MANIFEST, VALIDATED,
)
from warehouse_bigdata.contracts.validation import validate_dataset, validate_manifest
from warehouse_bigdata.forecasting.acceptance import validate_canonical_scope
from warehouse_bigdata.forecasting.feature_input import require_sealed
from warehouse_bigdata.optimization.decision_batch import (
    CONTRACTS, _validate_inputs, validate_decision_datasets,
)
from warehouse_bigdata.optimization.decision_inputs import (
    HISTORICAL_ISSUE_DATE, DecisionInputs, ExcludedSeries, plain,
)
from warehouse_bigdata.optimization.policy_evaluation import (
    BASELINE, PROPOSED, _json_value, validate_decision_freeze,
)
from warehouse_bigdata.optimization.replenishment import ForecastPrediction
from warehouse_bigdata.optimization.scenario_generation import EligiblePair, ObservedDailySale

DECISION_PRODUCER = 'warehouse-bigdata:inventory-decision-batch'
EVALUATION_PRODUCER = 'warehouse-bigdata:inventory-policy-evaluation'
_TYPES = {'String': 'string', 'Date': 'date', 'NonNegativeInt': 'bigint',
          'Integer': 'bigint', 'NonNegativeFloat': 'double', 'Bool': 'boolean'}


def _require(condition, message):
    if not condition:
        raise HdfsNamespaceError(message)


def physical_descriptor(contract):
    # Spark's inferred Parquet reader exposes every field as nullable. Required
    # logical nullability is checked on actual values by validate_dataset below.
    return [dict(name=f.name, type=_TYPES[f.logical_type], nullable=True) for f in contract.fields]


def validate_physical_descriptor(contract, descriptor):
    _require(descriptor == physical_descriptor(contract), 'inventory physical schema differs')


def _write_dataset(spark, uri, contract, rows):
    from pyspark.sql import types as T
    types = {'string': T.StringType, 'date': T.DateType, 'bigint': T.LongType,
             'double': T.DoubleType, 'boolean': T.BooleanType}
    schema = T.StructType([T.StructField(f.name, types[_TYPES[f.logical_type]](), f.nullable)
                           for f in contract.fields])
    values = [tuple(date.fromisoformat(r[f.name]) if f.logical_type == 'Date' else r[f.name]
                    for f in contract.fields) for r in rows]
    spark.createDataFrame(values, schema).coalesce(1).write.mode('errorifexists').parquet(uri)


def _read_dataset(spark, uri, contract):
    # Do not supply a schema: it would hide reordered/mistyped physical columns.
    frame = (spark.read.option('ignoreMissingFiles', 'false').option('ignoreCorruptFiles', 'false')
             .option('mergeSchema', 'true').parquet(uri))
    descriptor = [dict(name=f.name, type=f.dataType.simpleString(), nullable=f.nullable)
                  for f in frame.schema]
    validate_physical_descriptor(contract, descriptor)
    files = frame.inputFiles()
    _require(0 < len(files) <= 1024 and len(set(files)) == len(files), 'invalid Parquet file population')
    for path in files:
        physical = (spark.read.option('ignoreMissingFiles', 'false')
                    .option('ignoreCorruptFiles', 'false').parquet(path))
        validate_physical_descriptor(contract, [dict(name=f.name,
            type=f.dataType.simpleString(), nullable=f.nullable) for f in physical.schema])
    rows = frame.limit(201).collect()
    _require(len(rows) <= 200, 'inventory dataset exceeds bounded population')
    return [_json_value(r.asDict(recursive=True)) for r in rows], descriptor


def _input_document(inputs):
    return _json_value(dict(predictions=[asdict(r) for r in inputs.predictions],
        observed_sales=[asdict(r) for r in inputs.observed_sales],
        eligible_pairs=[asdict(r) for r in inputs.eligible_pairs],
        exclusions=[asdict(r) for r in inputs.exclusions], selector=plain(inputs.selector),
        scope=plain(inputs.scope), coverage=plain(inputs.coverage),
        upstream_evidence=plain(inputs.upstream_evidence)))


def _inputs(document):
    def dated(cls, row, key):
        return cls(**{**row, key: date.fromisoformat(row[key])})
    return DecisionInputs(tuple(dated(ForecastPrediction, {**r,
        'prediction_date': date.fromisoformat(r['prediction_date'])}, 'issue_date') for r in document['predictions']),
        tuple(dated(ObservedDailySale, r, 'date') for r in document['observed_sales']),
        tuple(EligiblePair(**r) for r in document['eligible_pairs']),
        tuple(ExcludedSeries(**r) for r in document['exclusions']),
        *(document[k] for k in ('selector', 'scope', 'coverage', 'upstream_evidence')))


def _frozen_document(batch):
    # This is exactly the existing policy freeze digest payload, not a new identity.
    return _json_value(dict(upstream_identities=plain(batch.inputs.selector), scope=plain(batch.inputs.scope),
        upstream_evidence=plain(batch.inputs.upstream_evidence),
        forecasts=[asdict(r) for r in batch.inputs.predictions], inventory=plain(batch.inventory_rows),
        supply=[asdict(r) for r in batch.supplies], requests=[asdict(r) for r in batch.requests],
        allocations=[asdict(r) for r in batch.allocations], decision_run_ids=batch.decision_run_ids,
        policies=[PROPOSED, BASELINE], policy_version='v1',
        issue_date=HISTORICAL_ISSUE_DATE,
        coverage=plain(batch.inputs.coverage), exclusions=[asdict(r) for r in batch.inputs.exclusions]))


def _contracts(kind):
    return CONTRACTS if kind == 'decision' else (INVENTORY_POLICY_EVALUATION,)


def _manifest_name(kind):
    return 'decision_manifest.json' if kind == 'decision' else 'evaluation_manifest.json'


def _final(config, binding):
    if binding['kind'] == 'decision':
        return render_decision_output_path(config.base_path, binding['decision_run_id'])
    return render_evaluation_output_path(config.base_path, binding['evaluation_run_id'])


def inventory_file_integrity(client, root, kind):
    datasets = {c.name for c in _contracts(kind)}
    excluded = {_manifest_name(kind), 'evidence/publication.json'}
    metadata = {'_scope.json', 'evidence/decision_binding.json', 'evidence/audit.json'}
    if kind == 'decision':
        metadata.add('scenario_manifest.json')
    inventory, seen, pending = [], set(), [root]
    while pending:
        parent = pending.pop()
        for status in client.list_status(parent):
            path = status['path']
            _require(str(PurePosixPath(path).parent) == parent and str(PurePosixPath(path)) == path
                     and path not in seen, 'inventory duplicate or non-child path')
            seen.add(path)
            _require(len(seen) <= 1024, 'inventory bundle exceeds bounded layout')
            relative = str(PurePosixPath(path).relative_to(root))
            if status['type'] == 'DIRECTORY':
                _require(relative in datasets | {'evidence'}, 'unexpected inventory directory')
                pending.append(path)
                continue
            name = PurePosixPath(relative).name
            parquet = str(PurePosixPath(relative).parent) in datasets and (
                name == '_SUCCESS' or (name.startswith('part-') and name.endswith('.parquet')))
            _require(status['type'] == 'FILE' and (relative in metadata | excluded or parquet),
                     'unexpected inventory file: ' + relative)
            if relative not in excluded:
                _require(type(status['length']) is int and status['length'] >= 0, 'invalid file length')
                inventory.append(dict(path=relative, size_bytes=status['length']))
    names = {r['path'] for r in inventory}
    _require(metadata <= names and all(any(p.startswith(n + '/part-') and p.endswith('.parquet')
        for p in names) for n in datasets), 'incomplete inventory bundle')
    inventory.sort(key=lambda r: r['path'])
    paths = [root + '/' + r['path'] for r in inventory]
    checksums = client.checksums(paths)
    _require(set(checksums) == set(paths) and all(checksums.values()), 'incomplete inventory checksums')
    for item, path in zip(inventory, paths, strict=True):
        item['checksum'] = checksums[path]
    return inventory


def _validate_decision(binding, audit, datasets):
    inputs = _inputs(audit['inputs'])
    _validate_inputs(inputs)
    _require(plain(inputs.scope) == binding['scope'] and plain(inputs.selector) == binding['upstream_identities'],
             'decision binding inputs differ')
    validate_decision_datasets(inputs=inputs, decision_run_id=binding['decision_run_id'],
        **dict(zip(('inventory_rows', 'central_supply_rows', 'replenishment_rows', 'allocation_rows'),
                   (datasets[c.name] for c in CONTRACTS), strict=True)))
    metadata = audit['decision_metadata']
    for key in ('decision_run_id', 'source_snapshot_id', 'forecast_run_id', 'model_run_id',
                'scenario_id', 'issue_date', 'policy_id', 'policy_version'):
        _require(metadata[key] == binding[key], 'decision metadata lineage differs: ' + key)
    _require(metadata['exclusions'] == audit['inputs']['exclusions']
             and metadata['coverage'] == audit['inputs']['coverage'], 'decision exclusions differ')
    scenario = audit['scenario_metadata']
    parameters = json.loads(scenario['scenario_parameters'])
    digest = document_digest(parameters)
    _require(digest == scenario['parameter_digest'] and scenario['scenario_id'] == 'historical_scaled:' + digest[7:]
             and scenario['scenario_id'] == binding['scenario_id'], 'scenario identity differs')
    for row in datasets[CONTRACTS[0].name]:
        _require(all(row[k] == scenario[k] for k in ('scenario_version', 'scenario_type',
            'parameter_digest', 'origin_label', 'scenario_seed')), 'scenario provenance differs')


def _pair(row):
    return row['item_id'], row['store_id']


def _unique(rows, key):
    result = {key(r): r for r in rows}
    _require(len(result) == len(rows), 'duplicate audit keys')
    return result


def _validate_evaluation(binding, audit, datasets):
    """Independent arithmetic checks over persisted outcomes and frozen evidence."""
    frozen, metadata = audit['frozen'], audit['metadata']
    from warehouse_bigdata.forecasting.evaluation import EvaluationContext
    selector = binding['upstream_identities']
    ctx = EvaluationContext(selector['test_evaluation_run_id'], selector['model_run_id'],
                            selector['source_snapshot_id'], selector['feature_version'], 'test')
    _require(binding['issue_date'] == HISTORICAL_ISSUE_DATE.isoformat()
             and binding['forecast_run_id'] == ctx.forecast_id(1913), 'historical origin identity differs')
    _require(document_digest(frozen) == metadata['decision_freeze_digest'], 'decision freeze digest differs')
    _require(frozen['upstream_identities'] == binding['upstream_identities']
             and frozen['scope'] == binding['scope'] and frozen['policies'] == [PROPOSED, BASELINE]
             and frozen['policy_version'] == 'v1', 'policy frozen lineage differs')
    ids = metadata['decision_run_ids']
    _require(list(frozen['decision_run_ids']) == [ids[PROPOSED], ids[BASELINE]]
             and ids[PROPOSED] != ids[BASELINE], 'policy decision IDs differ')
    for key in ('source_snapshot_id', 'forecast_run_id', 'model_run_id', 'evaluation_run_id', 'run_type', 'issue_date'):
        _require(metadata[key] == binding[key], 'evaluation lineage differs: ' + key)
    _require(metadata['run_type'] == 'test' and metadata['label_provenance'] == 'observed_m5_unit_sales_demand_proxy'
             and metadata['outcome_provenance'] == INVENTORY_POLICY_EVALUATION.provenance,
             'evaluation provenance differs')
    stock = _unique(frozen['inventory'], _pair)
    validate_dataset(CONTRACTS[0], frozen['inventory'])
    requests = _unique(frozen['requests'], _pair)
    supplies = _unique(frozen['supply'], lambda r: r['item_id'])
    forecasts = _unique(frozen['forecasts'], lambda r: (*_pair(r), r['horizon']))
    _require(set(stock) == set(requests) and set(forecasts) == {(*k, h) for k in stock for h in range(1, 8)},
             'frozen decision population differs')
    for row in [*frozen['forecasts'], *frozen['requests']]:
        _require(all(row[k] == binding[k] for k in ('source_snapshot_id', 'forecast_run_id', 'model_run_id', 'issue_date')),
                 'frozen forecast lineage differs')
    for row in frozen['forecasts']:
        _require(row['feature_version'] == selector['feature_version']
                 and row['prediction_date'] == (HISTORICAL_ISSUE_DATE + timedelta(days=row['horizon'])).isoformat(),
                 'frozen forecast dates/features differ')
    request_rows = [{**r, 'decision_run_id': ids[PROPOSED]} for r in frozen['requests']]
    validate_dataset(CONTRACTS[2], [{n: r[n] for n in CONTRACTS[2].field_names} for r in request_rows])
    for row in stock.values():
        _require(row['scenario_id'] == binding['scenario_id'] and row['issue_date'] == binding['issue_date'],
                 'frozen scenario differs')
        _require(all(type(row[n]) is int and row[n] >= 0 for n in ('opening_stock_units', 'safety_stock_units')),
                 'invalid frozen stock quantity')
    for key, request in requests.items():
        _require(request['scenario_id'] == binding['scenario_id']
                 and all(request[n] == stock[key][n] for n in ('opening_stock_units', 'safety_stock_units'))
                 and type(request['request_units']) is int and request['request_units'] >= 0,
                 'frozen request/stock reconciliation differs')
    _require(all(r['scenario_id'] == binding['scenario_id'] and r['issue_date'] == binding['issue_date']
                 and type(r['central_available_units']) is int and r['central_available_units'] >= 0
                 for r in supplies.values()), 'invalid frozen central supply')
    population = {_pair(r) for r in binding['scope']['selected_series']}
    exclusions = _unique(metadata['exclusions'], _pair)
    _require(all(r['reason_codes'] and r['stage'] in {'FORECAST', 'HISTORY', 'LABELS'} for r in exclusions.values()),
             'unexplained evaluation exclusions')
    _require([r for r in metadata['exclusions'] if r['stage'] != 'LABELS'] == frozen['exclusions']
             and frozen['coverage']['decision_eligible_series'] == len(stock)
             and frozen['coverage']['candidate_series'] == len(population), 'frozen exclusions/coverage differ')
    days = [(date.fromisoformat(binding['issue_date']) + timedelta(days=h)).isoformat() for h in range(1, 8)]
    labels = _unique(audit['labels'], lambda r: (*_pair(r), r['date']))
    _require(len(labels) <= 700, 'observed label evidence exceeds bounded population')
    _require(all(_pair(r) in stock and r['date'] in days and r['source_snapshot_id'] == binding['source_snapshot_id']
                 for r in labels.values()), 'observed label window or lineage differs')
    paired = {k for k in stock if all((*k, d) in labels and type(labels[(*k, d)]['unit_sales']) is int
                  and 0 <= labels[(*k, d)]['unit_sales'] <= 2**63 - 1 for d in days)}
    _require(paired.isdisjoint(exclusions) and paired | set(exclusions) == population,
             'paired population or exclusions differ')
    for key in set(stock) - paired:
        reason = 'INVALID_LABELS' if any(type(r['unit_sales']) is not int or not 0 <= r['unit_sales'] <= 2**63 - 1
                     for r in labels.values() if _pair(r) == key) else 'INCOMPLETE_LABELS'
        _require(exclusions[key]['stage'] == 'LABELS' and exclusions[key]['reason_codes'] == [reason],
                 'label exclusion reason differs')
    rows = datasets[INVENTORY_POLICY_EVALUATION.name]
    outcomes = _unique(rows, lambda r: (r['policy_id'], *_pair(r)))
    daily = _unique(audit['daily_rows'], lambda r: (r['policy_id'], *_pair(r), r['date']))
    _require(set(outcomes) == {(p, *k) for p in (PROPOSED, BASELINE) for k in paired}
             and set(daily) == {(p, *k, d) for p in (PROPOSED, BASELINE) for k in paired for d in days},
             'evaluation outcome/daily population differs')
    recomputed_metrics = {}
    for index, policy in enumerate((PROPOSED, BASELINE)):
        allocation = frozen['allocations'][index] if frozen['allocations'] else {'recommendations': [], 'central_remainders': []}
        allocations = _unique(allocation['recommendations'], _pair)
        _require(set(allocations) == set(stock), 'allocation population differs')
        remainders = dict(allocation['central_remainders'])
        _require(set(supplies) == {k[0] for k in stock} == set(remainders), 'supply population differs')
        for item, supply in supplies.items():
            used = sum(r['allocated_units'] for r in allocations.values() if r['item_id'] == item)
            _require(type(remainders[item]) is int and remainders[item] >= 0
                     and used + remainders[item] == supply['central_available_units'], 'per-SKU supply conservation differs')
        for key, allocation_row in allocations.items():
            request = requests[key]
            _require(all(allocation_row[n] == binding[n] for n in ('source_snapshot_id', 'forecast_run_id', 'model_run_id', 'scenario_id', 'issue_date'))
                     and allocation_row['policy_id'] == policy and allocation_row['policy_version'] == 'v1'
                     and type(allocation_row['allocated_units']) is int
                     and 0 <= allocation_row['allocated_units'] <= request['request_units']
                     and allocation_row['requested_units'] == request['request_units']
                     and allocation_row['unmet_request_units'] == request['request_units'] - allocation_row['allocated_units'],
                     'frozen allocation reconciliation differs')
        allocation_rows = [{**r, 'decision_run_id': ids[policy],
            'central_available_units': supplies[r['item_id']]['central_available_units']}
            for r in allocations.values()]
        validate_dataset(CONTRACTS[3], [{n: r[n] for n in CONTRACTS[3].field_names} for r in allocation_rows])
        policy_rows = []
        for key in sorted(paired):
            row = outcomes[(policy, *key)]
            _require(all(row[n] == binding[n] for n in ('evaluation_run_id', 'source_snapshot_id', 'forecast_run_id', 'scenario_id', 'issue_date', 'run_type'))
                     and row['decision_run_id'] == ids[policy] and row['policy_version'] == 'v1'
                     and row['observed_label_window_start'] == days[0] and row['observed_label_window_end'] == days[-1],
                     'outcome lineage differs')
            remaining = stock[key]['opening_stock_units'] + allocations[key]['allocated_units']
            fulfilled = unmet = stockouts = 0
            for day in days:
                d = daily[(policy, *key, day)]
                demand = labels[(*key, day)]['unit_sales']
                served = min(remaining, demand)
                _require(d['opening_stock_units'] == remaining and d['observed_sales_proxy_units'] == demand
                         and d['simulated_fulfilled_units'] == served and d['simulated_unmet_units'] == demand - served
                         and d['remaining_stock_units'] == remaining - served, 'daily inventory equation differs')
                remaining -= served
                fulfilled += served
                unmet += demand - served
                stockouts += int(demand > served)
            _require(all(row[n] == v for n, v in dict(actual_sales_proxy_7d_units=fulfilled + unmet,
                simulated_fulfilled_units=fulfilled, simulated_unmet_units=unmet, ending_stock_units=remaining,
                excess_over_safety_units=max(0, remaining - stock[key]['safety_stock_units']),
                simulated_stockout_days=stockouts).items()), 'final-stock/outcome reconciliation differs')
            policy_rows.append(row)
        totals = {n: sum(r[n] for r in policy_rows) for n in ('actual_sales_proxy_7d_units', 'simulated_fulfilled_units',
            'simulated_unmet_units', 'ending_stock_units', 'excess_over_safety_units', 'simulated_stockout_days')}
        demand = totals['actual_sales_proxy_7d_units']
        capacity = sum(r['central_available_units'] for r in supplies.values())
        allocated = sum(r['allocated_units'] for r in allocations.values())
        recomputed_metrics[policy] = dict(**totals, stockout_horizon_count=sum(r['simulated_unmet_units'] > 0 for r in policy_rows),
            unmet_request_units=sum(allocations[k]['unmet_request_units'] for k in paired),
            fulfillment_rate=totals['simulated_fulfilled_units'] / demand if demand else None,
            central_allocated_units=allocated, central_available_units=capacity,
            central_utilization=allocated / capacity if capacity else None,
            denominators=dict(paired_series=len(paired), paired_series_days=len(paired) * 7,
                observed_sales_proxy_units=demand, central_available_units=capacity, central_decision_series=len(stock)))
    recomputed_metrics['proposed_minus_baseline'] = {n: (recomputed_metrics[PROPOSED][n] - recomputed_metrics[BASELINE][n]
        if recomputed_metrics[PROPOSED][n] is not None and recomputed_metrics[BASELINE][n] is not None else None)
        for n in recomputed_metrics[PROPOSED] if n != 'denominators'}
    _require(recomputed_metrics == audit['metrics'], 'evaluation metrics differ')
    counts = Counter((r['stage'], code) for r in exclusions.values() for code in r['reason_codes'])
    coverage = {**frozen['coverage'], 'complete_label_series': len(paired), 'paired_series': len(paired),
        'evaluation_row_count': len(rows), 'daily_row_count': len(daily), 'excluded_series': len(exclusions),
        'exclusions_by_stage_reason': [dict(stage=s, reason_code=c, series_count=n) for (s, c), n in sorted(counts.items())]}
    _require(coverage == audit['coverage'], 'evaluation coverage differs')


def _readback(spark, client, config, root, binding, audit):
    validate_canonical_scope(binding['scope'])
    _require(read_scope(client, root) == binding['scope'], 'persisted inventory scope differs')
    validate_small_artifacts(binding['upstream_identities'], through='j06' if binding['kind'] == 'decision' else 'test')
    _require(all(binding[k] == binding['upstream_identities'][k]
                 for k in ('source_snapshot_id', 'forecast_run_id', 'model_run_id')), 'selected upstream lineage differs')
    _require(binding['scope']['selection_digest'] == binding['upstream_identities']['selection_digest']
             and binding['configuration_digest'] == document_digest(binding['effective_configuration'])
             and binding['upstream_evidence_digest'] == document_digest(audit['inputs']['upstream_evidence']
                 if binding['kind'] == 'decision' else audit['frozen']['upstream_evidence']), 'binding digest differs')
    datasets, descriptors = {}, {}
    for contract in _contracts(binding['kind']):
        rows, schema = _read_dataset(spark, config.uri + root + '/' + contract.name, contract)
        validate_dataset(contract, rows)
        datasets[contract.name] = rows
        descriptors[contract.name] = dict(row_count=len(rows), schema_fingerprint=document_digest({'schema': schema}),
            provenance=contract.provenance, logical_unique_key=list(contract.unique_key))
    if binding['kind'] == 'decision':
        _validate_decision(binding, audit, datasets)
    else:
        _validate_evaluation(binding, audit, datasets)
    return descriptors


def _make_manifest(config, binding, quality, audit, *, created_at, completed_at):
    kind = binding['kind']
    contract = DECISION_MANIFEST if kind == 'decision' else EVALUATION_MANIFEST
    datasets = _contracts(kind)
    final = _final(config, binding)
    uris = [config.uri + final + '/' + c.name for c in datasets]
    checksums = {uri: document_digest({'files': [r for r in quality['file_integrity']
        if r['path'].startswith(c.name + '/')]}) for uri, c in zip(uris, datasets, strict=True)}
    coverage = dict(datasets=quality['datasets'], input_binding_digest=document_digest(binding),
        publication_evidence_digest=document_digest(quality), audit_digest=document_digest(audit),
        scenario_id=binding['scenario_id'], upstream_identities=binding['upstream_identities'],
        pipeline_run_id=binding['pipeline_run_id'], coverage=audit.get('coverage', audit.get('decision_metadata', {}).get('coverage')),
        evidence_uri=config.uri + final + '/evidence/audit.json', publication_complete=True)
    if kind == 'evaluation':
        coverage.update(decision_run_ids=audit['metadata']['decision_run_ids'], policies=[PROPOSED, BASELINE],
                        decision_freeze_digest=audit['metadata']['decision_freeze_digest'])
    manifest = dict(manifest_schema_version=contract.version, contract_name=contract.name,
        contract_version=contract.version, manifest_kind=contract.manifest_kind, status=VALIDATED,
        producer=DECISION_PRODUCER if kind == 'decision' else EVALUATION_PRODUCER,
        artifact_uri=config.uri + final, artifact_checksum=document_digest({'files': quality['file_integrity']}),
        schema_fingerprint=document_digest({'datasets': quality['datasets']}),
        grain=datasets[-1].grain, logical_unique_key=','.join(datasets[-1].unique_key),
        row_count=sum(s['row_count'] for s in quality['datasets'].values()),
        required_field_null_counts=json.dumps({c.name: {n: 0 for n in c.required_field_names} for c in datasets}, sort_keys=True),
        code_revision=binding['code_revision'], configuration_digest=binding['configuration_digest'],
        created_at=created_at, completed_at=completed_at, coverage_and_exclusion_counts=json.dumps(coverage, sort_keys=True),
        source_snapshot_id=binding['source_snapshot_id'], model_run_id=binding['model_run_id'],
        forecast_run_id=binding['forecast_run_id'], scenario_id=binding['scenario_id'])
    if kind == 'decision':
        manifest.update({k: binding[k] for k in ('decision_run_id', 'issue_date', 'policy_id', 'policy_version')})
        manifest.update(decision_artifact_uris=json.dumps(uris), decision_artifact_checksums=json.dumps(checksums, sort_keys=True))
    else:
        from warehouse_bigdata.forecasting.evaluation import EvaluationContext
        selector = binding['upstream_identities']
        ctx = EvaluationContext(selector['test_evaluation_run_id'], selector['model_run_id'],
            selector['source_snapshot_id'], selector['feature_version'], 'test')
        manifest.update(evaluation_run_id=binding['evaluation_run_id'], holdout_id=ctx.holdout_id,
            run_type='test', evaluation_start=(date.fromisoformat(binding['issue_date']) + timedelta(days=1)).isoformat(),
            evaluation_end=(date.fromisoformat(binding['issue_date']) + timedelta(days=7)).isoformat(),
            evaluation_artifact_uris=json.dumps([*uris, coverage['evidence_uri']]),
            evaluation_artifact_checksums=json.dumps({**checksums, coverage['evidence_uri']: document_digest(audit)}, sort_keys=True))
    manifest = bind_scope(manifest, binding['scope'])
    validate_manifest(contract, manifest, lineage_context={'required_status': VALIDATED})
    return manifest


def _scenario_manifest(config, binding, audit, created_at):
    metadata = audit['scenario_metadata']
    contract = SCENARIO_MANIFEST
    manifest = {k: metadata[k] for k in contract.required_fields if k in metadata}
    manifest.update(manifest_schema_version=contract.version, contract_name=contract.name,
        contract_version=contract.version, manifest_kind=contract.manifest_kind, status=VALIDATED,
        producer=DECISION_PRODUCER, artifact_uri=config.uri + _final(config, binding),
        artifact_checksum=document_digest({'scenario': metadata}), schema_fingerprint=document_digest(
            {'schemas': [physical_descriptor(c) for c in CONTRACTS[:2]]}),
        grain=CONTRACTS[0].grain, logical_unique_key=','.join(CONTRACTS[0].unique_key),
        row_count=sum(metadata['datasets'][c.name]['row_count'] for c in CONTRACTS[:2]),
        required_field_null_counts=json.dumps({c.name: {n: 0 for n in c.required_field_names} for c in CONTRACTS[:2]}),
        code_revision=binding['code_revision'], configuration_digest=metadata['configuration_digest'],
        created_at=created_at, completed_at=created_at, coverage_and_exclusion_counts=json.dumps(
            {'scenario_metadata_digest': document_digest(metadata)}))
    manifest = bind_scope(manifest, binding['scope'])
    validate_manifest(contract, manifest, lineage_context={'required_status': VALIDATED})
    return manifest


def _persist(spark, *, client, config, pipeline_run_id, kind, inputs, rows, audit,
             identities, code_revision, effective_configuration):
    validate_publication_namespace(client, config)
    staging = render_staging_path(config.base_path, pipeline_run_id)
    status = client.path_status(staging)
    _require(status is not None and status['type'] == 'DIRECTORY' and status['owner'] == 'hadoop'
             and not int(status['permission'], 8) & 0o002 and client.can_access(staging, 'rwx'),
             'owner-prepared writable staging is required')
    _require(not (set(client.list_children(staging)) - {staging + '/events'}),
             'staging contains execution evidence; use owner recovery')
    binding = dict(schema_version='inventory-input-binding-v1', kind=kind, pipeline_run_id=pipeline_run_id,
        scope=plain(inputs.scope), upstream_identities=plain(inputs.selector), code_revision=code_revision,
        effective_configuration=effective_configuration, configuration_digest=document_digest(effective_configuration),
        upstream_evidence_digest=document_digest(plain(inputs.upstream_evidence)), **identities)
    _require(not client.exists(_final(config, binding)), 'immutable inventory artifact already exists')
    created_at = datetime.now(timezone.utc).isoformat()
    client.upload_bytes(_bytes(binding), staging + '/input_binding.json')
    try:
        client.mkdir_exclusive(staging + '/candidates')
        root = staging + '/candidates/inventory'
        client.mkdir_exclusive(root)
        client.mkdir_exclusive(root + '/evidence')
        write_scope(client, root, binding['scope'])
        client.upload_bytes(_bytes(binding), root + '/evidence/decision_binding.json')
        client.upload_bytes(_bytes(audit), root + '/evidence/audit.json')
        for contract, values in zip(_contracts(kind), rows, strict=True):
            validate_dataset(contract, values)
            _write_dataset(spark, config.uri + root + '/' + contract.name, contract, values)
        # Independent readers consume persisted metadata and bytes, never producer frames.
        saved_binding = _json(client, root + '/evidence/decision_binding.json')
        saved_audit = _json(client, root + '/evidence/audit.json')
        descriptors = _readback(spark, client, config, root, saved_binding, saved_audit)
        _require(all(descriptors[c.name]['row_count'] == len(values)
                     for c, values in zip(_contracts(kind), rows, strict=True)), 'persisted row counts differ')
        if kind == 'decision':
            client.upload_bytes(_bytes(_scenario_manifest(config, binding, audit, created_at)), root + '/scenario_manifest.json')
        quality = dict(status=VALIDATED, persisted_parquet_validated=True,
            input_binding_digest=document_digest(binding), datasets=descriptors,
            file_integrity=inventory_file_integrity(client, root, kind))
        manifest = _make_manifest(config, binding, quality, audit, created_at=created_at,
                                  completed_at=datetime.now(timezone.utc).isoformat())
        client.upload_bytes(_bytes(quality), root + '/evidence/publication.json')
        client.upload_bytes(_bytes(quality), staging + '/quality_report.json')
        # The completion marker is deliberately absent from the candidate.
        client.mkdir_exclusive(staging + '/publication')
        client.upload_bytes(_bytes(dict(version=1, pipeline_run_id=pipeline_run_id, manifest=manifest)),
                            staging + '/publication/request.json')
        return manifest
    except Exception as error:
        if not client.exists(staging + '/failure_report.json'):
            client.upload_bytes(_bytes(dict(status='FAILED', error=str(error))), staging + '/failure_report.json')
        raise


def persist_decision_bundle(spark, *, client, config, pipeline_run_id, inputs, result,
                            code_revision, effective_configuration):
    metadata = plain(result.decision_manifest_inputs)
    identities = {k: metadata[k] for k in ('decision_run_id', 'source_snapshot_id', 'forecast_run_id',
        'model_run_id', 'scenario_id', 'issue_date', 'policy_id', 'policy_version')}
    return _persist(spark, client=client, config=config, pipeline_run_id=pipeline_run_id, kind='decision',
        inputs=inputs, rows=[plain(r) for r in (result.inventory_rows, result.central_supply_rows,
            result.replenishment_rows, result.allocation_rows)],
        audit=dict(inputs=_input_document(inputs), decision_metadata=metadata,
                   scenario_metadata=plain(result.scenario_manifest_inputs)), identities=identities,
        code_revision=code_revision, effective_configuration=effective_configuration)


def persist_policy_evaluation_bundle(spark, *, client, config, pipeline_run_id, frozen, labels,
                                     result, code_revision, effective_configuration):
    validate_decision_freeze(frozen)
    metadata = plain(result.metadata)
    scenarios = {r['scenario_id'] for r in frozen.inventory_rows}
    _require(len(scenarios) == 1, 'policy publication requires one frozen scenario identity')
    scenario_id = next(iter(scenarios))
    identities = {k: metadata[k] for k in ('evaluation_run_id', 'source_snapshot_id', 'forecast_run_id',
        'model_run_id', 'issue_date', 'run_type')}
    identities.update(scenario_id=scenario_id, proposed_decision_run_id=frozen.decision_run_ids[0],
                      baseline_decision_run_id=frozen.decision_run_ids[1])
    audit = dict(frozen=_frozen_document(frozen), labels=_json_value(list(labels)),
        daily_rows=plain(result.daily_rows), metrics=plain(result.metrics),
        coverage=plain(result.coverage), metadata=metadata)
    return _persist(spark, client=client, config=config, pipeline_run_id=pipeline_run_id, kind='evaluation',
        inputs=frozen.inputs, rows=[plain(result.rows)], audit=audit, identities=identities,
        code_revision=code_revision, effective_configuration=effective_configuration)


def _validate_bundle(spark, client, config, path, manifest, *, pipeline_run_id=None):
    binding = _json(client, path + '/evidence/decision_binding.json')
    audit = _json(client, path + '/evidence/audit.json')
    quality = _json(client, path + '/evidence/publication.json')
    _require(binding['schema_version'] == 'inventory-input-binding-v1' and binding['kind'] in {'decision', 'evaluation'},
             'unsupported inventory binding')
    _require(pipeline_run_id is None or binding['pipeline_run_id'] == pipeline_run_id, 'inventory execution identity differs')
    scope = read_scope(client, path)
    validate_scope_binding(manifest, scope)
    _require(scope == binding['scope'], 'inventory frozen scope differs')
    descriptors = _readback(spark, client, config, path, binding, audit)
    _require(quality['status'] == VALIDATED and quality['persisted_parquet_validated'] is True
             and quality['input_binding_digest'] == document_digest(binding) and quality['datasets'] == descriptors,
             'inventory persisted validation differs')
    _require(inventory_file_integrity(client, path, binding['kind']) == quality['file_integrity'], 'inventory integrity differs')
    rebuilt = _make_manifest(config, binding, quality, audit,
        created_at=manifest['created_at'], completed_at=manifest['completed_at'])
    _require(rebuilt == manifest, 'inventory manifest differs from fresh readback')
    if binding['kind'] == 'decision':
        _require(_json(client, path + '/scenario_manifest.json') == _scenario_manifest(
            config, binding, audit, manifest['created_at']), 'scenario manifest differs')
    return binding, quality


def _require_sealed_bundle(client, config, path, kind, *, completed=True):
    for relative in ['evidence/publication.json',
                     *([_manifest_name(kind)] if completed else []),
                     *(r['path'] for r in inventory_file_integrity(client, path, kind))]:
        require_sealed(client, path + '/' + relative, config.base_path)


def verify_inventory_bundle(spark, *, client, config, path):
    binding = _json(client, path + '/evidence/decision_binding.json')
    _require(path == _final(config, binding), 'verifier requires the exact explicit identity path')
    kind = binding['kind']
    _require_sealed_bundle(client, config, path, kind)
    manifest = _json(client, path + '/' + _manifest_name(kind))
    _validate_bundle(spark, client, config, path, manifest)
    return manifest


def _owner_spark():
    from pyspark.sql import SparkSession
    return (SparkSession.builder.master('local[1]').appName('warehouse-j07-owner-readback')
        .config('spark.ui.enabled', 'false').config('spark.sql.session.timeZone', 'UTC')
        .getOrCreate())


def finalize_inventory_publication(client, config, *, pipeline_run_id, manifest):
    require_owner(client)
    validate_publication_namespace(client, config)
    staging = render_staging_path(config.base_path, pipeline_run_id)
    binding = _json(client, staging + '/input_binding.json')
    _require(binding['pipeline_run_id'] == pipeline_run_id, 'inventory request identity differs')
    kind = binding['kind']
    final, candidate = _final(config, binding), staging + '/candidates/inventory'
    envelope = _envelope(final, pipeline_run_id)
    claimed, pending = envelope + '/candidate', envelope + '/manifest.json'
    marker = final + '/' + _manifest_name(kind)
    exists = lambda p: client.path_status(p) is not None
    _require(not (exists(candidate) and exists(claimed)), 'concurrent candidate and claim')
    _require(not (exists(final) and (exists(candidate) or exists(claimed))), 'candidate and immutable inventory both exist')
    _require(_json(client, staging + '/quality_report.json')['status'] == VALIDATED,
             'inventory quality report is not VALIDATED')
    if exists(pending):
        _require(_json(client, pending) == manifest, 'pending inventory manifest conflicts')
    if exists(envelope):
        _require_private(client, envelope)
    source = final if exists(final) else claimed if exists(claimed) else candidate
    _require((client.path_status(source) or {}).get('type') == 'DIRECTORY', 'complete inventory candidate is missing')
    _require(source == final or not exists(source + '/' + _manifest_name(kind)),
             'candidate already contains a completion manifest')
    spark = _owner_spark()
    try:
        saved_binding, quality = _validate_bundle(spark, client, config, source, manifest, pipeline_run_id=pipeline_run_id)
        _require(saved_binding == binding and quality == _json(client, staging + '/quality_report.json'),
                 'staging inventory evidence differs')
        if exists(marker):
            _require(_json(client, marker) == manifest and not exists(pending), 'immutable inventory manifest conflicts')
            _require_sealed_bundle(client, config, final, kind)
            if exists(envelope) or client.path_status(final)['permission'] != '555':
                client.set_permission(final, '555')
                if exists(envelope):
                    client.remove(envelope)
                _seal(client, staging)
                return VALIDATED
            return 'REUSED'
        if not exists(final):
            _freeze_feature_metadata(client, staging)
            _require_private(client, envelope)
            if not exists(claimed):
                client.rename_exclusive(candidate, claimed)
            _seal(client, claimed)
            _validate_bundle(spark, client, config, claimed, manifest, pipeline_run_id=pipeline_run_id)
            _require_sealed_bundle(client, config, claimed, kind, completed=False)
            client.rename_exclusive(claimed, final)
        else:
            _require_sealed_bundle(client, config, final, kind, completed=False)
            _require_private(client, envelope)
        if not exists(pending):
            client.upload_bytes(_bytes(manifest), pending)
        _require(_json(client, pending) == manifest, 'inventory completion manifest readback differs')
        _seal(client, pending)
        # The final directory remains non-writable: publish marker by an owner-only
        # rename, briefly granting only its owner W, never Spark or shared ACLs.
        client.set_permission(final, '755')
        client.rename_exclusive(pending, marker)
        client.set_permission(final, '555')
        client.remove(envelope)
        _seal(client, staging)
        return VALIDATED
    finally:
        spark.stop()


def validate_inventory_recovery(args, binding):
    expected = {**{k: v for k, v in binding['upstream_identities'].items()
                  if k not in {'schema_version', 'dataset_scope'}}, **{k: binding[k] for k in (
        'pipeline_run_id', 'scenario_id', 'issue_date', 'configuration_digest')},
        **{k: binding.get(k) for k in ('decision_run_id', 'evaluation_run_id', 'run_type',
            'policy_id', 'policy_version', 'proposed_decision_run_id', 'baseline_decision_run_id')},
        **binding['effective_configuration'], 'code_revision': binding['code_revision']}
    for key, value in expected.items():
        # Selector path is an immutable upstream file, not a J07 ID extension.
        _require(getattr(args, key, None) == value, 'inventory recovery binding differs: ' + key)
