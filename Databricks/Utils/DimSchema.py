from pyspark.sql.types import *

dim_Employer_Schema_version = 1
dim_Employer_Schema =  StructType([

 StructField("EmployerKey", StringType(), True),
  StructField("employer_name", StringType(), True),
   StructField("employer_reviews", StringType(), True),
    StructField("employer_website", StringType(), True),
     StructField("country", StringType(), True),
      StructField("Slot", StringType(), True),
       StructField("Submission", StringType(), True),
        StructField("SourceSystem", StringType(), True),

])

