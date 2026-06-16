from pyspark.sql import SparkSession

from Utils.Read_Write_Helper import get_adls, read_parquet


def _cleansed_dim_employer_path(data_provider: str, slot: str, submission: str) -> str:
    return f"{get_adls()}Cleansed/{data_provider}/dim_Employer/{slot}/{submission}"


def _integrated_dim_employer_path(data_provider: str, slot: str, submission: str) -> str:
    return f"{get_adls()}Integrated/{data_provider}/dim_Employer/{slot}/{submission}"


def process(run):
    """Integrate Cleansed dim_Employer into Integrated layer."""
    spark = SparkSession.builder.getOrCreate()

    data_provider = run.get_dateprovider()
    slot = run.get_slot()
    submission = run.get_submission()

    df = read_parquet(spark, _cleansed_dim_employer_path(data_provider, slot, submission))
    df_integrated = df.dropDuplicates(["EmployerKey"])

    df_integrated.write.mode("overwrite").parquet(
        _integrated_dim_employer_path(data_provider, slot, submission)
    )
