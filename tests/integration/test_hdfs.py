"""Opt-in T03 verification against an already initialized, running T01 cluster."""

import pytest

from tests.integration.hdfs_support import HDFSVerification

pytestmark = pytest.mark.integration


def test_independent_client_registration_replication_and_integrity(hdfs: HDFSVerification) -> None:
    hdfs.verify_fixture("baseline")


@pytest.mark.hdfs_disruptive
def test_fixture_persists_after_daemon_restart(hdfs: HDFSVerification) -> None:
    hdfs.check_persistence(recreate=False)


@pytest.mark.hdfs_disruptive
def test_fixture_persists_after_forced_container_recreation(hdfs: HDFSVerification) -> None:
    hdfs.check_persistence(recreate=True)


@pytest.mark.hdfs_disruptive
def test_degraded_read_and_datanode_recovery(hdfs: HDFSVerification) -> None:
    hdfs.check_interruption()
