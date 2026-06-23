from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from Utils.Read_Write_Helper import get_adls, read_parquet,apply_schema
from Utils.DimSchema import dim_Benefits_Schema_version


def _integrated_dim_benefits_path(data_provider: str, slot: str, submission: str) -> str:
    return f"{get_adls()}Integrated/{data_provider}/dim_Job_Benefits/{slot}/{submission}"


def _presented_dim_benefits_path(data_provider: str, slot: str, submission: str) -> str:
    return f"{get_adls()}Presented/{data_provider}/dim_Job_Benefits/{slot}/{submission}"


def process(run):
    """Present Integrated dim_benefitsfor reporting."""
    spark = SparkSession.builder.getOrCreate()

    data_provider = run.get_dateprovider()
    slot = run.get_slot()
    submission = run.get_submission()

    df = read_parquet(spark, _integrated_dim_benefits_path(data_provider, slot, submission))

    presented_path = f'{get_adls()}Presented/dim_Job_Benefits/Schema={dim_Benefits_Schema_version}'


    if not DeltaTable.isDeltaTable(spark, presented_path) : 
         df.write.mode("overwrite").format("delta").save(presented_path)
    else :
        target = DeltaTable.forPath(spark, presented_path)
        target.alias('target').merge(df.alias('source'),
        'source.EmployerKey = target.EmployerKey  AND target.job_id = source.job_id' ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    


