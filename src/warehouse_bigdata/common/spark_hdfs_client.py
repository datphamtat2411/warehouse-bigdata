"""Driver HDFS CLI access with an unprivileged NameNode live-node report."""

from __future__ import annotations

import json
from urllib.parse import urlsplit
from urllib.request import ProxyHandler, build_opener

from warehouse_bigdata.common.hdfs_client import (
    EXPECTED_DATANODES,
    DataNodeReport,
    HdfsCliClient,
    HdfsCommandError,
)

_NAMENODE_INFO = "Hadoop:service=NameNode,name=NameNodeInfo"


class SparkHdfsCliClient(HdfsCliClient):
    """Keep file integrity checks on HDFS without requiring DFSAdmin privileges.

    NameNodeInfo exposes the current live registrations and transfer addresses.
    Contact/report fields are explicitly labeled as elapsed ages in this adapter.
    Owner-side clients retain the standard CLI-based DFSAdmin implementation.
    """

    def report(self) -> dict[str, DataNodeReport]:
        """Read bounded monitoring metadata from this CLI's configured NameNode."""
        try:
            rpc = urlsplit(self.getconf("fs.defaultFS"))
            http = urlsplit("//" + self.getconf("dfs.namenode.http-address"))
            if (
                rpc.scheme != "hdfs"
                or not rpc.hostname
                or http.hostname not in {rpc.hostname, "0.0.0.0", "::"}
                or http.port is None
                or http.username is not None
                or http.password is not None
                or http.path
                or http.query
                or http.fragment
            ):
                raise ValueError("monitoring endpoint does not match the configured NameNode")
            host = f"[{rpc.hostname}]" if ":" in rpc.hostname else rpc.hostname
            url = f"http://{host}:{http.port}/jmx?qry={_NAMENODE_INFO}"
            opener = build_opener(ProxyHandler({}))
            with opener.open(url, timeout=min(self.command_timeout, 5.0)) as response:
                payload = response.read(self.metadata_limit + 1)
            if len(payload) > self.metadata_limit:
                raise HdfsCommandError("NameNode live-node report exceeded the bounded limit.")
            document = json.loads(payload)
            beans = document["beans"]
            if not isinstance(beans, list) or len(beans) != 1 or beans[0]["name"] != _NAMENODE_INFO:
                raise ValueError("expected exactly one NameNodeInfo bean")
            live = json.loads(beans[0]["LiveNodes"])
            if not isinstance(live, dict) or len(live) != len(EXPECTED_DATANODES):
                raise ValueError("expected exactly two live DataNodes")
            nodes: dict[str, DataNodeReport] = {}
            addresses: set[str] = set()
            for name, info in live.items():
                hostname, port = name.rsplit(":", 1)
                if hostname not in EXPECTED_DATANODES or hostname in nodes or port != "9866":
                    raise ValueError(f"unexpected or duplicate live DataNode: {name!r}")
                if info["adminState"] != "In Service":
                    raise ValueError(f"DataNode {hostname!r} is not in normal service")
                address = info["xferaddr"]
                transfer = urlsplit("//" + address)
                if (
                    not transfer.hostname
                    or transfer.port != 9866
                    or transfer.username is not None
                    or transfer.password is not None
                    or transfer.path
                    or transfer.query
                    or transfer.fragment
                    or address in addresses
                ):
                    raise ValueError(f"invalid or duplicate DataNode transfer address: {address!r}")
                ages = (info["lastContact"], info["lastBlockReport"])
                if any(type(age) is not int or age < 0 for age in ages):
                    raise ValueError(f"invalid DataNode contact/report age: {hostname!r}")
                nodes[hostname] = DataNodeReport(
                    hostname=hostname,
                    address=address,
                    last_contact=f"{ages[0]} seconds ago",
                    last_block_report=f"{ages[1]} seconds ago",
                )
                addresses.add(address)
            return nodes
        except HdfsCommandError:
            raise
        except (OSError, ValueError, KeyError, TypeError) as error:
            raise HdfsCommandError(f"Cannot read NameNode live-node report: {error}") from error
