"""Explicit M02 T03 acceptance against existing T01/T02 infrastructure."""

import pytest

from tests.integration.hdfs_support import HDFSVerification
from tests.integration.spark_support import verify_spark_hdfs

pytestmark = pytest.mark.integration


def test_distributed_spark_hdfs_round_trip(hdfs: HDFSVerification) -> None:
    verify_spark_hdfs(hdfs)
