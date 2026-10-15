"""
bronze_table.py  (AWS Glue Spark job, one job per table; ADR-007)

Reads every release of one table's CSV files from landing/<table>/ and
writes Parquet to bronze/<table>/, partitioned by release.
- Every column stays text (no type inference), as in the Snowflake raw layer.
- Column names are converted to snake_case, because Parquet and Athena do not
  accept the spaces and punctuation in CMS headers.
- Adds lineage: _source_file, _ingested_at.
- Fails if the table has no rows.

Job arguments: --table --lake_root (s3://bucket) [--encoding UTF-8]
"""
from pyspark.sql import functions as F

from lakehouse_utils import get_args, snake, spark_session, write_table

args = get_args(["table", "lake_root", "encoding"], defaults={"encoding": "UTF-8"})
table, root = args["table"], args["lake_root"].rstrip("/")
spark = spark_session(f"bronze_{table}")

df = (spark.read
      .option("header", True)
      .option("inferSchema", False)
      .option("multiLine", True)
      .option("escape", '"')
      .option("encoding", args["encoding"])
      .option("mode", "PERMISSIVE")
      .csv(f"{root}/landing/{table}/"))

renamed, used = [], set()
for c in df.columns:
    if c == "release":
        renamed.append(F.col("release"))
        continue
    new = snake(c)
    while new in used:
        new += "_x"
    used.add(new)
    renamed.append(F.col(f"`{c}`").alias(new))

df = (df.select(*renamed)
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingested_at", F.current_timestamp()))

rows = write_table(spark, df, "cms_bronze", table, f"{root}/bronze/{table}/", partition="release")
if rows == 0:
    raise RuntimeError(f"bronze {table}: no rows written")
