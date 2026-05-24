from pyspark.sql import SparkSession
from pyspark.sql.functions import explode_outer, lit

from Utils.Read_Write_Helper import get_adls


def transform_df(df, slot, submission, dataprovider):
    df = (
        df.withColumn("Slot", lit(slot))
        .withColumn("Submission", lit(submission))
        .withColumn("SourceSystem", lit(dataprovider))
    )
    return df


def process(run):
    """Generate Sourced Fact Job."""
    spark = SparkSession.builder.getOrCreate()

    data_provider = run.get_dateprovider()
    slot = run.get_slot()
    submission = run.get_submission()

    df = spark.read.json(
        f"{get_adls()}Landed/{data_provider}/{slot}/{submission}/"
    )

    jobs_df = df.select(
        "request_id",
        "status",
        "parameters.country",
        "parameters.query",
        "parameters.date_posted",
        explode_outer("data.jobs").alias("job"),
    )

    job_flatten = jobs_df.select(
        "request_id",
        "status",
        "country",
        "query",
        "date_posted",
        "job.*",
    )

    job_flatten_apply_links = (
        job_flatten.select("*", explode_outer("apply_options").alias("apply_option"))
        .select("*", "apply_option.*")
    )
    job_flatten_apply_links = job_flatten_apply_links.drop(
        "apply_option",
        "job_employment_types",
        "apply_options",
    )

    sourced_df = job_flatten_apply_links.select(
        "country",
        "employer_name",
        "employer_reviews",
        "employer_website",
        "job_apply_is_direct",
        "job_apply_link",
        "job_benefits",
        "job_city",
        "job_country",
        "job_description",
        "job_employment_type",
        "job_google_link",
        "job_id",
        "job_is_remote",
        "job_latitude",
        "job_location",
        "job_longitude",
        "job_max_salary",
        "job_min_salary",
        "job_onet_job_zone",
        "job_onet_soc",
        "job_posted_at_datetime_utc",
        "job_posted_at_timestamp",
        "job_publisher",
        "job_salary",
        "job_salary_period",
        "job_salary_string",
        "job_state",
        "job_title",
        "apply_link",
        "is_direct",
        "publisher",
    )

    sourced_df.write.mode("overwrite").parquet(
        f"{get_adls()}Sourced/{data_provider}/{slot}/{submission}/"
    )
