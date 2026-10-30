# Spark 3.5 + Iceberg + Kafka + S3 image used by streaming jobs, batch jobs and tests.
FROM python:3.11-slim-bookworm
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl procps \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PYSPARK_PYTHON=python \
    PYTHONUNBUFFERED=1
RUN pip install --no-cache-dir pyspark==3.5.3
ARG MVN=https://repo1.maven.org/maven2
RUN J=$(python -c "import pyspark,os;print(os.path.join(os.path.dirname(pyspark.__file__),'jars'))") \
    && cd $J \
    && curl -fsSLO --retry 5 --retry-all-errors $MVN/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.1/iceberg-spark-runtime-3.5_2.12-1.6.1.jar \
    && curl -fsSLO --retry 5 --retry-all-errors $MVN/org/apache/iceberg/iceberg-aws-bundle/1.6.1/iceberg-aws-bundle-1.6.1.jar \
    && curl -fsSLO --retry 5 --retry-all-errors $MVN/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar \
    && curl -fsSLO --retry 5 --retry-all-errors $MVN/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar \
    && curl -fsSLO --retry 5 --retry-all-errors $MVN/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
    && curl -fsSLO --retry 5 --retry-all-errors $MVN/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
    && curl -fsSLO --retry 5 --retry-all-errors $MVN/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar
WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir ".[stream]"
ENV SETTLELAKE_HOME=/data
