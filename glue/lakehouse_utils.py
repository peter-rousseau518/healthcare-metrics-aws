"""
lakehouse_utils.py: helpers shared by the bronze, silver, and gold Glue jobs.
Shipped to Glue with --extra-py-files.
"""
import re
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


def get_args(names, defaults=None):
    """Read --name value job arguments (Glue passes extra system arguments too)."""
    defaults = defaults or {}
    out = {}
    for n in names:
        flag = f"--{n}"
        if flag in sys.argv:
            out[n] = sys.argv[sys.argv.index(flag) + 1]
        elif n in defaults:
            out[n] = defaults[n]
        else:
            raise ValueError(f"Missing job argument {flag}")
    return out


def spark_session(app_name: str) -> SparkSession:
    spark = (SparkSession.builder.appName(app_name).enableHiveSupport().getOrCreate())
    spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    spark.conf.set("spark.sql.ansi.enabled", "false")
    return spark


def snake(name: str) -> str:
    """'CMS Certification Number (CCN)' -> 'cms_certification_number_ccn'"""
    s = re.sub(r"[^0-9a-zA-Z]+", "_", name.strip().lstrip("\ufeff")).strip("_").lower()
    return f"c_{s}" if s and s[0].isdigit() else (s or "unnamed")


def write_table(spark: SparkSession, df: DataFrame, database: str, table: str, path: str,
                partition: str | None = None, decimals_to_double: bool = False) -> int:
    """Overwrite Parquet at `path` and (re)register it as an external Hive table
    in the Glue Data Catalog, so Athena can query it."""
    if decimals_to_double:
        for f in df.schema.fields:
            if isinstance(f.dataType, DecimalType):
                df = df.withColumn(f.name, F.col(f.name).cast("double"))
    writer = df.write.mode("overwrite")
    if partition:
        writer = writer.partitionBy(partition)
    writer.parquet(path)

    cols = [f for f in df.schema.fields if f.name != partition]
    col_ddl = ",\n  ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in cols)
    spark.sql(f"DROP TABLE IF EXISTS {database}.{table}")
    spark.sql(
        f"CREATE EXTERNAL TABLE {database}.{table} (\n  {col_ddl}\n)"
        + (f"\nPARTITIONED BY (`{partition}` string)" if partition else "")
        + f"\nSTORED AS PARQUET\nLOCATION '{path}'"
    )
    if partition:
        spark.sql(f"MSCK REPAIR TABLE {database}.{table}")
    rows = spark.table(f"{database}.{table}").count()
    print(f"WROTE {database}.{table}: {rows:,} rows -> {path}")
    return rows
