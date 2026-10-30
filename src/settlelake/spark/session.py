"""SparkSession factory. The same code runs against three Iceberg catalogs:

  local  Hadoop catalog on a local directory   (unit tests, laptops)
  jdbc   JDBC catalog (Postgres) + S3FileIO    (docker compose with MinIO)
  glue   AWS Glue catalog + S3FileIO           (AWS; queryable from Athena)

Configured through environment variables (see docs/runbook.md).
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

CATALOG = "lake"


def build_spark(app: str = "settlelake", **overrides: str) -> SparkSession:
    kind = os.environ.get("SETTLELAKE_CATALOG", "local")
    warehouse = os.environ.get("SETTLELAKE_WAREHOUSE", "/tmp/settlelake-warehouse")
    b = (
        SparkSession.builder.appName(app)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", warehouse)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "2g"))
    )
    cfg = f"spark.sql.catalog.{CATALOG}"
    if kind == "local":
        b = b.config(f"{cfg}.type", "hadoop")
    else:
        b = b.config(f"{cfg}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        if kind == "jdbc":
            b = (
                b.config(f"{cfg}.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")
                .config(f"{cfg}.uri", os.environ["SETTLELAKE_JDBC_URI"])
                .config(f"{cfg}.jdbc.user", os.environ["SETTLELAKE_JDBC_USER"])
                .config(f"{cfg}.jdbc.password", os.environ["SETTLELAKE_JDBC_PASSWORD"])
            )
        elif kind == "glue":
            b = b.config(f"{cfg}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        else:
            raise ValueError(f"unknown SETTLELAKE_CATALOG {kind!r}")
        if os.environ.get("SETTLELAKE_S3_ENDPOINT"):  # MinIO / S3-compatible
            b = b.config(f"{cfg}.s3.endpoint", os.environ["SETTLELAKE_S3_ENDPOINT"]).config(
                f"{cfg}.s3.path-style-access", "true"
            )
    for k, v in overrides.items():
        b = b.config(k, v)
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
