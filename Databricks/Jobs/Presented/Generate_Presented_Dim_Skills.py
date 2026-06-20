from Utils.Read_Write_Helper import get_adls, read_parquet
from pyspark.sql import SparkSession
from Utils.DimSchema import dim_Job_Skills_Schema_version



def _presented_dim_skills_path(data_provider: str, slot: str, submission: str) -> str:
    return f"{get_adls()}Presented//dim_Job_Skills/Schema={dim_Job_Skills_Schema_version}"


def _integrated_dim_skills_path(data_provider: str, slot: str, submission: str) -> str:
    return f"{get_adls()}Integrated/{data_provider}/dim_Job_Skills/{slot}/{submission}"

def process(run):
    spark = SparkSession.builder.getOrCreate()
    data_provider = run.get_dateprovider()
    slot = run.get_slot()
    submission = run.get_submission()
    df = read_parquet(spark, _integrated_dim_skills_path(data_provider, slot, submission))
    


    df.write.mode("append").format('delta').save(_presented_dim_skills_path(data_provider, slot, submission))
