from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode_outer, lit

from Utils.KeysHelper import generate_employer_key
from Utils.Read_Write_Helper import get_adls, read_parquet_sourced


def generate_dim_employer(df, data_provider, slot, submission):
    df_employee = df.select(
        [
            "EmployerKey",
            "employer_name",
            "employer_reviews",
            "employer_website",
            "country",
            "Slot",
            "Submission",
            "SourceSystem",
        ]
    )
    df_employee.write.mode("overwrite").parquet(
        f"{get_adls()}Cleansed/{data_provider}/dim_Employer/{slot}/{submission}"
    )


def generate_dim_job_benefits(df, data_provider, slot, submission):
    df_dim_job_benefits = df.filter(col("job_benefits").isNotNull())
    df_dim_job_benefits = (
        df_dim_job_benefits.select(
            "job_id",
            "EmployerKey",
            explode_outer("job_benefits").alias("benefits"),
            "Slot",
            "Submission",
            "SourceSystem",
        )
        .groupBy("EmployerKey", "job_id", "Slot", "Submission", "SourceSystem")
        .pivot("benefits")
        .agg(lit(1))
        .fillna(0)
    )
    df_dim_job_benefits.write.mode("overwrite").parquet(
        f"{get_adls()}Cleansed/{data_provider}/dim_Job_Benefits/{slot}/{submission}"
    )


def generate_dim_job_description(df, data_provider, slot, submission):
    df_job_description = df.select(
        "job_id",
        "EmployerKey",
        "job_description",
        "Slot",
        "Submission",
        "SourceSystem",
    )
    df_job_description.write.mode("overwrite").parquet(
        f"{get_adls()}Cleansed/{data_provider}/dim_Job_Description/{slot}/{submission}"
    )

def process(run):
    """Generate Cleansed Fact Job."""
    spark = SparkSession.builder.getOrCreate()

    data_provider = run.get_dateprovider()
    slot = run.get_slot()
    submission = run.get_submission()

    # Read source data
    df = read_parquet_sourced(spark, run=run)

    # Generate Employer Key
    df_employee_key = generate_employer_key(df, data_provider)

    # Generate Dim Employer data
    generate_dim_employer(df_employee_key, data_provider, slot, submission)

    df_employee_key_after_dim_employer = df_employee_key.drop(

        "country",
        "employer_name",
        "employer_reviews",
        "employer_website",
        )
    df_employee_key_after_dim_employer = df_employee_key_after_dim_employer.drop(
        "job_apply_is_direct", "job_apply_link", "job_publisher"
    )

    df_employee_key_after_dim_employer = df_employee_key_after_dim_employer.select(
    [
        "job_id",
        "EmployerKey",
        "job_title",
        "job_employment_type",
        "job_posted_at_datetime_utc",
        "job_posted_at_timestamp",
        "job_is_remote",
        "job_salary",
        "job_salary_period",
        "job_salary_string",
        "job_max_salary",
        "job_min_salary",
        "job_benefits",
        "apply_link",
        "is_direct",
        "publisher",
        #  -----------
        "job_description",
        "job_google_link",
        "job_latitude",
        "job_location",
        "job_longitude",
        "job_onet_job_zone",
        "job_onet_soc",
        "job_city",
        "job_state",
        "job_country",
        "Slot",
        "Submission",
        "SourceSystem"
    ])
    df_employee_key_after_dim_employer = df_employee_key_after_dim_employer.drop('job_latitude','job_location','job_longitude')
    # Generate job description
    generate_dim_job_description(
        df_employee_key_after_dim_employer,
        data_provider,
        slot,
        submission,
    )
    df_employee_key_after_dim_employer = df_employee_key_after_dim_employer.drop(
        "job_description"
    )


    # Generate job benefits
    generate_dim_job_benefits(
        df_employee_key_after_dim_employer,
        data_provider,
        slot,
        submission,
    )
    df_employee_key_after_dim_employer = df_employee_key_after_dim_employer.drop(
        "job_benefits"
    )

    df_employee_key_after_dim_employer.write.mode("overwrite").parquet(
        f"{get_adls()}Cleansed/{data_provider}/Fact_Job/{slot}/{submission}"
    )





