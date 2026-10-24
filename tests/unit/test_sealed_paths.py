"""Security and coverage of call-local sealed publication permission batches."""

from collections import Counter
from pathlib import PurePosixPath

import pytest

from warehouse_bigdata.forecasting.feature_input import (
    FeatureInputError,
    require_sealed,
    require_sealed_paths,
)

BASE = "/data/warehouse"
ROOT = BASE + "/outputs/forecasts/run/forecast_daily"
PATHS = [ROOT + "/part-0.parquet", ROOT + "/part-1.parquet"]


def ancestors(paths):
    result = set()
    for path in paths:
        for current in (PurePosixPath(path), *PurePosixPath(path).parents):
            result.add(str(current))
            if str(current) == BASE:
                break
    return result


class SealedClient:
    def __init__(self):
        self.statuses = {p: {"owner": "hadoop", "permission": "555"} for p in ancestors(PATHS)}
        self.acls = {p: "user::r-x\ngroup::r-x\nother::r-x\n" for p in self.statuses}
        self.status_calls = Counter()
        self.acl_calls = Counter()

    def path_status(self, path):
        self.status_calls[path] += 1
        return self.statuses.get(path)

    def get_acl(self, path):
        self.acl_calls[path] += 1
        return self.acls[path]


def test_batch_preserves_singleton_coverage_and_checks_duplicates_and_shared_ancestors_once():
    baseline, batched = SealedClient(), SealedClient()
    paths = [*PATHS, PATHS[0], ROOT, BASE]
    for path in paths:
        require_sealed(baseline, path, BASE)
    require_sealed_paths(batched, iter(paths), BASE)
    expected = Counter(dict.fromkeys(ancestors(paths), 1))
    assert batched.status_calls == batched.acl_calls == expected
    assert set(baseline.status_calls) == set(baseline.acl_calls) == set(expected)
    assert sum(baseline.acl_calls.values()) > sum(batched.acl_calls.values())
    require_sealed_paths(batched, paths, BASE)
    assert batched.status_calls == batched.acl_calls == Counter({p: 2 for p in expected})


@pytest.mark.parametrize("target", [PATHS[0], ROOT, BASE])
@pytest.mark.parametrize(
    "field,value",
    [("owner", "spark"), ("permission", "575"), ("permission", "557")],
)
def test_batch_rejects_wrong_ownership_and_each_shared_write_bit(target, field, value):
    client = SealedClient()
    client.statuses[target][field] = value
    with pytest.raises(FeatureInputError, match="not sealed"):
        require_sealed_paths(client, PATHS, BASE)
    assert target not in client.acl_calls
    assert PATHS[1] not in client.status_calls


@pytest.mark.parametrize("target", [PATHS[0], ROOT, BASE])
@pytest.mark.parametrize(
    "acl",
    ["user:reader:r--", "group:readers:r--", "default:user::rwx", "mask::r-x"],
)
def test_batch_rejects_named_default_and_mask_acls_at_every_level(target, acl):
    client = SealedClient()
    client.acls[target] += "  " + acl + "\n"
    with pytest.raises(FeatureInputError, match="shared ACLs"):
        require_sealed_paths(client, PATHS, BASE)
    assert client.acl_calls[target] == 1
    assert PATHS[1] not in client.status_calls


@pytest.mark.parametrize("failure", ["missing", "status_denied", "acl_denied", "bad_mode"])
def test_batch_fails_closed_and_does_not_reuse_partial_success(monkeypatch, failure):
    client = SealedClient()
    if failure == "missing":
        del client.statuses[ROOT]
    elif failure == "bad_mode":
        client.statuses[ROOT]["permission"] = "invalid"
    else:
        method = "path_status" if failure == "status_denied" else "get_acl"
        original = getattr(client, method)

        def denied(path):
            if path == ROOT:
                raise PermissionError("metadata denied")
            return original(path)

        monkeypatch.setattr(client, method, denied)
    with pytest.raises((ValueError, PermissionError)):
        require_sealed_paths(client, PATHS, BASE)
    assert PATHS[1] not in client.status_calls
    monkeypatch.undo()
    client.statuses[ROOT] = {"owner": "hadoop", "permission": "555"}
    require_sealed_paths(client, PATHS, BASE)
    assert client.status_calls[PATHS[0]] == client.acl_calls[PATHS[0]] == 2


@pytest.mark.parametrize("mutation", ["owner", "mode", "acl"])
def test_new_boundary_rechecks_shared_parents_after_mutation(mutation):
    client = SealedClient()
    require_sealed_paths(client, PATHS, BASE)
    if mutation == "owner":
        client.statuses[BASE]["owner"] = "spark"
    elif mutation == "mode":
        client.statuses[BASE]["permission"] = "577"
    else:
        client.acls[BASE] += "default:group::rwx\n"
    with pytest.raises(FeatureInputError):
        require_sealed_paths(client, PATHS, BASE)
    assert client.status_calls[BASE] == 2
