from pyspark.sql import SparkSession
from Utils.Read_Write_Helper import get_adls, read_parquet

def _cleansed_fact_job_path(data_provider: str, slot: str, submission: str) -> str:
    return f"{get_adls()}Cleansed/{data_provider}/Fact_Job/{slot}/{submission}"

def _integrated_fact_job_path(data_provider: str, slot: str, submission: str) -> str:
    return f"{get_adls()}Integrated/{data_provider}/Fact_Job/{slot}/{submission}"

def process(run):
      
    """Generate Integrated Fact Job."""
    spark = SparkSession.builder.getOrCreate()

    data_provider = run.get_dateprovider()
    slot = run.get_slot()
    submission = run.get_submission()
    df = read_parquet(spark, _cleansed_fact_job_path(data_provider, slot, submission))
    df.write.mode("overwrite").parquet(_integrated_fact_job_path(data_provider, slot, submission))

    
    


  


