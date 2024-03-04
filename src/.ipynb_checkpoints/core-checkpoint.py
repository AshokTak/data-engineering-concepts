from datetime import date, datetime
from typing import List, Optional

from pyspark import SparkConf
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.utils import AnalysisException


def build_spark_session(
    application_name: str, 
    master: str = 'yarn',
    checkpoint_path: str = 'hdfs:///checkpoints',
) -> SparkSession:
    """Build and return a SparkSession optionally include additional python files.

    :param application_name: name of the spark application
    :param master: URI of the master
    :param checkpoint_path: directory in a distributed filesystem to store Spark checkpoints
    :returns SparkSession:
    """
    conf: SparkConf = SparkConf() \
        .setAppName(application_name) \
        .setMaster(master)

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.setCheckpointDir(checkpoint_path)

    return spark


def parse_processing_date(date_str: Optional[str] = None) -> date:
    """Returns the given date_str as a date type."""
    if not date_str:
        return datetime.utcnow().date()
    return datetime.strptime(date_str, "%Y-%m-%d").date()


def save_dataframe_as_table(
    output_df: DataFrame, 
    table: str, 
    s3_bucket: str, 
    partition_keys: List[str],
    output_format: str
):
    """
    For the given partitioned table, `df.insertInto()` is attempted first.
    If `df.insertInto()` fails, then `df.saveAsTable()` is attempted.

    See comments for the performance improvements of attempting `df.insertInto()` first.
    """
    path = f's3://{s3_bucket}/{table}/'
    df_writer = output_df.write \
        .format(output_format) \
        .mode('overwrite') \
        .option('path', path)

    # Using `df.saveAsTable()` will *recreate* a table if exists.
    # This causes AWS Glue to recrawl the table location for all existing partitions.
    # So overtime, the runtime of `df.saveAsTable()` increases due to this recrawl.
    # Therefore, we attempt `df.insertInto()` first on tables with partition_keys.
    try:
        df_writer.insertInto(table, overwrite=True)
    except AnalysisException:
        # Exception is likely due to:
        # 1. Table does not exist.
        # 2. Or the table schema has changed.
        # Use saveAsTable() to recreate the table.
        df_writer.saveAsTable(table, partitionBy=partition_keys)
