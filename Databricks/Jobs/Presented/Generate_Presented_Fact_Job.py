
from pyspark.sql import SparkSession
from Utils.Read_Write_Helper import get_adls, read_parquet

def _integrated_fact_job_path(data_provider: str, slot: str, submission: str) -> str:
    return f"{get_adls()}Integrated/{data_provider}/Fact_Job/{slot}/{submission}"

def process(run):
    """Generate Presented Fact Job."""
    spark = SparkSession.builder.getOrCreate()
    data_provider = run.get_dateprovider()
    slot = run.get_slot()
    submission = run.get_submission()
    df = read_parquet(spark, _integrated_fact_job_path(data_provider, slot, submission))
    df = df.dropDuplicates()

    presented_path = f"{get_adls()}/Presented/Fact_Job/Schema={fact_Job_Schema_Version}"
    if not DeltaTable.isDeltaTable(spark, presented_path) : 
         df.write.mode("append").partitionBy('SourceSystem','publisher').format("delta").save(presented_path)
    else :
        df_presented = spark.read.format('delta').load(presented_path)
        new = df.join(df_presented, on=['job_id','EmployerKey'], how='left-anti')
        df.write.mode("append").partitionBy('SourceSystem','publisher').format("delta").save(presented_path)

    

