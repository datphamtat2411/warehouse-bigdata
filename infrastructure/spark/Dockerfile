# Shared Master/Worker runtime; the bundled Spark distribution is not replaced.
FROM ghcr.io/astral-sh/uv:0.13.1@sha256:4a1f3666816dafadd20ceb71eac9a7d56d7ef30279f7d64048519ab901ada336 AS uv
FROM spark:3.5.7-scala2.12-java11-ubuntu@sha256:e119521e81658967567bfcb6f9c66d5a7349b6975669ad3af7ccd7b1b5759c47

USER root
COPY --from=uv /uv /usr/local/bin/uv

# Pin and independently verify the artifact before uv's managed installation.
# uv also verifies its embedded download checksum when consuming the local mirror.
RUN set -eu; \
    archive=cpython-3.11.17+20261013-x86_64-unknown-linux-gnu-install_only_stripped.tar.gz; \
    mirror=/tmp/python-mirror/20261013; \
    mkdir -p "$mirror"; \
    wget -q --user-agent=uv/0.13.1 --timeout=30 --tries=3 -O "$mirror/$archive" \
      'https://releases.astral.sh/github/python-build-standalone/releases/download/20261013/cpython-3.11.17%2B20261013-x86_64-unknown-linux-gnu-install_only_stripped.tar.gz'; \
    printf '%s  %s\n' \
      '3b8e5a248bcc351265ffe68f5e457668a2e09c99a47f81f658988c7d3d107522' \
      "$mirror/$archive" | sha256sum -c -; \
    uv python install --no-cache --no-bin --install-dir /opt/python-managed \
      --mirror file:///tmp/python-mirror 3.11.17; \
    ln -s /opt/python-managed/cpython-3.11.17-linux-x86_64-gnu /opt/python; \
    rm -rf /tmp/python-mirror

ENV PATH="/opt/python/bin:${PATH}" \
    PYSPARK_PYTHON=/opt/python/bin/python3.11 \
    PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip \
    PYTHONDONTWRITEBYTECODE=1 \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    SPARK_DAEMON_MEMORY=256m \
    SPARK_DAEMON_JAVA_OPTS="-Xms128m -Djava.io.tmpdir=/tmp" \
    SPARK_LOCAL_DIRS=/tmp/warehouse-spark/local \
    SPARK_WORKER_DIR=/tmp/warehouse-spark/work \
    SPARK_LOG_DIR=/tmp/warehouse-spark/logs \
    SPARK_PID_DIR=/tmp/warehouse-spark \
    SPARK_PRINT_LAUNCH_COMMAND=1

COPY --chown=185:185 healthcheck.py /opt/warehouse-spark/healthcheck.py
COPY --chown=185:185 scratch.py /opt/warehouse-spark/scratch.py
USER 185:185
WORKDIR /tmp
ENTRYPOINT ["/usr/bin/tini", "--", "/opt/python/bin/python3.11", "/opt/warehouse-spark/scratch.py"]
