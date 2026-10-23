"""Read-only bounded historical policy evaluation; no publication or regeneration."""

from warehouse_bigdata.optimization.decision_inputs import read_historical_inputs, read_historical_labels
from warehouse_bigdata.optimization.policy_evaluation import (
    evaluate_frozen_policies, freeze_policy_decisions,
)


def run_inventory_policy_evaluation(spark, *, client, config, evaluation_run_id,
                                    proposed_decision_run_id, baseline_decision_run_id):
    inputs = read_historical_inputs(spark, client=client, config=config)
    frozen = freeze_policy_decisions(inputs, proposed_decision_run_id=proposed_decision_run_id,
                                     baseline_decision_run_id=baseline_decision_run_id)
    labels = read_historical_labels(spark, frozen=frozen, client=client, config=config)
    return evaluate_frozen_policies(frozen, labels, evaluation_run_id=evaluation_run_id)
