# Driver-only application layer over T01's unchanged, qualified runtime.
FROM warehouse-bigdata-spark:3.5.7-python3.11.17

USER root
WORKDIR /opt/warehouse-app
COPY pyproject.toml uv.lock README.md ./
COPY src/ ./src/
COPY scripts/package-spark-app.py /opt/warehouse-spark/package-spark-app.py
RUN uv sync --locked --no-dev --no-editable --python /opt/python/bin/python3.11 \
    && /opt/python/bin/python3.11 /opt/warehouse-spark/package-spark-app.py \
       /opt/warehouse-app/src /opt/warehouse-app/warehouse_bigdata.zip

ENV PATH="/opt/warehouse-app/.venv/bin:${PATH}" \
    PYSPARK_DRIVER_PYTHON=/opt/warehouse-app/.venv/bin/python \
    PYSPARK_PYTHON=/opt/python/bin/python3.11
USER 185:185
WORKDIR /tmp
ENTRYPOINT ["/usr/bin/tini", "-g", "--", "/opt/warehouse-app/.venv/bin/warehouse-bigdata", "spark-submit"]
CMD []
