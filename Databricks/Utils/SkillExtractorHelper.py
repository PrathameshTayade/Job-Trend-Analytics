from huggingface_hub import snapshot_download
from pyspark.sql import Row
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql import SparkSession
import spacy


def load_model():
    model_path = snapshot_download("amjad-awad/skill-extractor", repo_type="model")
    nlp = spacy.load(model_path)
    return nlp

def extract_skills(df,spark):
    
    nlp = load_model()
    rows = []
    for row in df.collect():
        doc = nlp(row['job_description'])
        skills = list(
            set(
            ent.text
            for ent in doc.ents
            if "SKILLS" in ent.label_
        )
        )
        rows.append(
             Row(
                Job_id=row["job_id"],
                EmployerKey=row["EmployerKey"],
                Job_description=row["job_description"],
                Skills=skills,
                Slot = row["Slot"],
                Submission = row["Submission"],
                SourceSystem = row["SourceSystem"]
            )
        )
    df_skills = spark.createDataFrame(rows)
    return df_skills

