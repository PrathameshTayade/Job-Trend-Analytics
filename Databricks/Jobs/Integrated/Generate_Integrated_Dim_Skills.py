from Utils.Read_Write_Helper import get_adls, read_parquet
from Utils.SkillExtractorHelper import  load_model, extract_skills
from pyspark.sql import SparkSession
from Utils.DimSchema import dim_Job_Skills_Schema_version
from delta.tables import DeltaTable



def _cleansed_dim_skills_path(data_provider: str, slot: str, submission: str) -> str:
    return f"{get_adls()}Cleansed/{data_provider}/dim_Job_Description/{slot}/{submission}"


def _integrated_dim_skills_path(data_provider: str, slot: str, submission: str) -> str:
    return f"{get_adls()}Integrated/{data_provider}/dim_Job_Skills/{slot}/{submission}"



def process(run): 
    spark = SparkSession.builder.getOrCreate()

    data_provider = run.get_dateprovider()
    slot = run.get_slot()
    submission = run.get_submission()
    df = read_parquet(spark, _cleansed_dim_skills_path(data_provider, slot, submission))

    presented_path = f"{get_adls()}Presented/dim_Job_Skills/Schema={dim_Job_Skills_Schema_version}"

    if not DeltaTable.isDeltaTable(spark, presented_path):
        df_skills = extract_skills(df,spark)
        df_skills.write.mode("overwrite").parquet(_integrated_dim_skills_path(data_provider, slot, submission))
    else:
        df_presented = read_parquet(spark, presented_path)
        new_df = df.join(df_presented, on=["EmployerKey","job_id"], how="left-anti")
        df_skills = extract_skills(new_df,spark)
        df_skills.write.mode("overwrite").parquet(_integrated_dim_skills_path(data_provider, slot, submission))



       



