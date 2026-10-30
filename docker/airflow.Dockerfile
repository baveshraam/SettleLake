# Airflow (dev/standalone mode) with Java + PySpark + Iceberg jars + the settlelake package.
FROM apache/airflow:2.10.5-python3.11
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
USER airflow
RUN pip install --no-cache-dir pyspark==3.5.3
ARG MVN=https://repo1.maven.org/maven2
RUN J=$(python -c "import pyspark,os;print(os.path.join(os.path.dirname(pyspark.__file__),'jars'))") \
    && cd $J \
    && curl -fsSLO $MVN/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.1/iceberg-spark-runtime-3.5_2.12-1.6.1.jar \
    && curl -fsSLO $MVN/org/apache/iceberg/iceberg-aws-bundle/1.6.1/iceberg-aws-bundle-1.6.1.jar \
    && curl -fsSLO $MVN/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar
WORKDIR /opt/settlelake
COPY --chown=airflow:root pyproject.toml README.md ./
COPY --chown=airflow:root src ./src
RUN pip install --no-cache-dir ".[stream]"
COPY --chown=airflow:root orchestration/dags /opt/airflow/dags
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False \
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=False \
    SETTLELAKE_HOME=/data
