#!/usr/bin/env bash
set -euo pipefail

export IVY_HOME=/home/spark/.ivy2
export MAVEN_OPTS="-Dmaven.repo.local=/home/spark/.m2/repository"

# optionally wait a short time for dependencies
sleep 2

echo "Effective envs: PACKAGES='${PACKAGES:-}' WAREHOUSE='${WAREHOUSE:-}'"

# Build packages arg only if PACKAGES is defined and non-empty
PACKAGES_ARG=""
if [ -n "${PACKAGES:-}" ]; then
  # Remove newlines just in case and trim spaces
  CLEAN_PACKAGES=$(echo "${PACKAGES}" | tr -d '\r\n' | tr -s ' ')
  PACKAGES_ARG="--packages ${CLEAN_PACKAGES}"
fi

# ensure AWS_REGION is set (falls back to AWS_DEFAULT_REGION or us-east-1)
: "${AWS_REGION:=${AWS_DEFAULT_REGION:-us-east-2}}"

# Build common spark-submit options
SPARK_OPTS=(
  --master local[*]
  ${PACKAGES_ARG}
  --conf spark.driver.memory="${SPARK_DRIVER_MEMORY:-1g}"
  --conf spark.executor.memory="${SPARK_EXECUTOR_MEMORY:-1g}"
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
  --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog
  --conf spark.sql.catalog.iceberg.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog
  --conf spark.sql.catalog.iceberg.warehouse="${WAREHOUSE:-s3a://fq-app-analytics-bucket-1/iceberg-warehouse/}"
  --conf "spark.driver.extraJavaOptions=-Daws.region=${AWS_REGION}" \
  --conf "spark.executor.extraJavaOptions=-Daws.region=${AWS_REGION}" \
  --conf "spark.hadoop.fs.s3a.region=${AWS_REGION}" \
)


echo "Running spark-submit with: ${SPARK_OPTS[*]} /app/src/runner_mongo_ingestion.py"

# Exec the job (replace current PID 1)
exec /opt/spark/bin/spark-submit "${SPARK_OPTS[@]}" /app/src/runner_mongo_ingestion.py

# entrypoint - prevents --packages from being added when PACKAGES is empty or malformed.
# It strips newlines and excessive whitespace.
# It prints what it will run — helpful for debugging.
