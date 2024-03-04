import defopt
from datetime import date, timedelta

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame, SparkSession

import core
from stories_metadata import data_sources, udfs


def transform(
        spark: SparkSession,
        s3_bucket: str,
        output_stories_metadata_table: str,
        input_stories_table: str,
        processing_dt: date
):
    # Read input table as dataframes
    stories_df = spark.read.table(input_stories_table)
    stories_metadata_df = spark.read.table(output_stories_metadata_table)

    # Build the output dataframe
    output_stories_metadata_df = build_output_df(
        processing_dt, stories_df, stories_metadata_df)

    # Save the output dataframe
    core.save_dataframe_as_table(
        stories_metadata_df,
        table=output_stories_metadata_df,
        s3_bucket=s3_bucket,
        partition_keys=data_sources.stories_metadata["partition_by"])


def build_output_df(
    processing_dt: date,
    input_stories_df: DataFrame,
    input_stories_metadata_df: DataFrame
) -> DataFrame:

    # Implement this function according to the given requirements
    # of your coding interview question.

    output_df = input_stories_df.withColumn("language", udfs.lang_detect(input_stories_df["content"]))
    
    output_df = output_df.withColumn("dt", F.to_date(F.lit(processing_dt), 'yyyy-MM-dd'))
    #output_df.drop('content', 'last_modified_ts')
    output_df = output_df.select('id', 'language', 'dt')
    return output_df


def main(
    *,
    s3_bucket: str,
    output_stories_metadata_table: str,
    input_stories_table: str,
    processing_date: str
):
    processing_dt = core.parse_processing_date(processing_date)

    spark = core.build_spark_session(output_stories_metadata_table)

    try:
        transform(
            spark,
            s3_bucket,
            output_stories_metadata_table,
            input_stories_table,
            processing_dt)

    finally:
        spark.stop()


if __name__ == '__main__':
    defopt.run(main)
