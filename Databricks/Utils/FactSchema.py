from pyspark.sql.types import *

fact_Job_Schema_Version = 1

fact_Job_Schema = StructType([
    StructField("job_id", StringType(), True),
    StructField("EmployerKey", StringType(), True),
    StructField("job_title", StringType(), True),
    StructField("job_employment_type", StringType(), True),
    StructField("job_posted_at_datetime_utc", StringType(), True),
    StructField("job_posted_at_timestamp", LongType(), True),
    StructField("job_is_remote", BooleanType(), True),
    StructField("job_salary", StringType(), True),
    StructField("job_salary_period", StringType(), True),
    StructField("job_salary_string", StringType(), True),
    StructField("job_max_salary", LongType(), True),
    StructField("job_min_salary", LongType(), True),
    StructField("apply_link", StringType(), True),
    StructField("is_direct", BooleanType(), True),
    StructField("publisher", StringType(), True),
    StructField("job_google_link", StringType(), True),
    StructField("job_onet_job_zone", StringType(), True),
    StructField("job_onet_soc", StringType(), True),
    StructField("job_city", StringType(), True),
    StructField("job_state", StringType(), True),
    StructField("job_country", StringType(), True),
    StructField("Slot", StringType(), True),
    StructField("Submission", StringType(), True),
    StructField("SourceSystem", StringType(), True),
])