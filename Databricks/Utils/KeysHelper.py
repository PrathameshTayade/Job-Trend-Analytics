from pyspark.sql.functions import coalesce, col, concat, lit, sha1


# Generate employer key
def generate_employer_key(df, source_system):
    df_with_key = df.withColumn(
        "EmployerKey",
        sha1(
            concat(
                coalesce(col("employer_name"), lit("NULL")),
                coalesce(col("employer_website"), lit("NULL")),
                coalesce(col("SourceSystem"), lit(source_system)),
            )
        ),
    )
    return df_with_key