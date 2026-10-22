# Driver-only application layer over T01's unchanged, qualified runtime.
FROM apache/hadoop:3.4.3@sha256:127774dadab40ce84df7ac668a7a8c99945688b3fe336f1388f4477ca33e1529 AS hadoop-client
FROM warehouse-bigdata-spark:3.5.7-python3.11.17

USER root
# The official HDFS CLI has its own libraries; Spark keeps its bundled client JARs.
COPY --from=hadoop-client /opt/hadoop/bin/hdfs /opt/hadoop/bin/hdfs
COPY --from=hadoop-client /opt/hadoop/libexec/ /opt/hadoop/libexec/
COPY --from=hadoop-client /opt/hadoop/etc/hadoop/ /opt/hadoop/etc/hadoop/
COPY --from=hadoop-client /opt/hadoop/share/hadoop/common/ /opt/hadoop/share/hadoop/common/
COPY --from=hadoop-client /opt/hadoop/share/hadoop/hdfs/ /opt/hadoop/share/hadoop/hdfs/
COPY infrastructure/spark/hadoop-cli-log4j.properties /opt/hadoop/etc/hadoop/log4j.properties

WORKDIR /opt/warehouse-app
COPY pyproject.toml uv.lock README.md ./
COPY src/ ./src/
COPY scripts/package-spark-app.py /opt/warehouse-spark/package-spark-app.py
RUN uv sync --locked --no-dev --no-editable --python /opt/python/bin/python3.11 \
    && /opt/python/bin/python3.11 /opt/warehouse-spark/package-spark-app.py \
       /opt/warehouse-app/src /opt/warehouse-app/warehouse_bigdata.zip

ENV PATH="/opt/warehouse-app/.venv/bin:/opt/hadoop/bin:${PATH}" \
    HADOOP_HOME=/opt/hadoop \
    HADOOP_YARN_HOME=/opt/hadoop \
    HADOOP_MAPRED_HOME=/opt/hadoop \
    HADOOP_HEAPSIZE_MAX=256 \
    PYSPARK_DRIVER_PYTHON=/opt/warehouse-app/.venv/bin/python \
    PYSPARK_PYTHON=/opt/python/bin/python3.11
USER 185:185
WORKDIR /tmp
ENTRYPOINT ["/usr/bin/tini", "-g", "--", "/opt/warehouse-app/.venv/bin/warehouse-bigdata", "spark-submit"]
CMD []
