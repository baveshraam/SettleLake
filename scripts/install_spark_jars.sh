#!/usr/bin/env bash
# Download the Iceberg/Kafka/S3/JDBC jars next to an installed pyspark (CI and local development).
set -euo pipefail
J=$(python -c "import pyspark,os;print(os.path.join(os.path.dirname(pyspark.__file__),'jars'))")
MVN=https://repo1.maven.org/maven2
cd "$J"
for u in \
  org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.1/iceberg-spark-runtime-3.5_2.12-1.6.1.jar \
  org/apache/iceberg/iceberg-aws-bundle/1.6.1/iceberg-aws-bundle-1.6.1.jar \
  org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar \
  org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar \
  org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
  org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
  org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar; do
  [ -f "$(basename "$u")" ] || curl -fsSLO "$MVN/$u"
done
echo "jars installed in $J"
