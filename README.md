```

## Key Features

- Ingests job posting data from REST API sources using Azure Data Factory pipelines.
- Stores raw and transformed data in Azure Data Lake Storage using a layered medallion-style architecture.
- Uses Databricks and PySpark to flatten nested JSON, clean records, generate keys, and prepare fact and dimension datasets.
- Builds analytics-ready tables for jobs, employers, job benefits, and extracted skills.
- Applies NLP-based skill extraction from job descriptions using a Hugging Face/spaCy model.
- Stores presented-layer datasets in Delta Lake for efficient querying and reporting.
- Includes Azure DevOps CI/CD pipelines for validating and deploying ADF, Databricks, and SQL assets.
- Provides a Power BI model for analyzing job trends and market insights.

## Technology Stack

- Azure Data Factory
- Azure Data Lake Storage
- Azure Databricks
- PySpark
- Delta Lake
- Azure Key Vault
- Azure DevOps
- Databricks Asset Bundles
- SQL
- Power BI
- Python
- spaCy / Hugging Face

## Data Lake Layers

| Layer | Purpose |
| --- | --- |
| Landed | Stores raw API responses as received from the source system. |
| Sourced | Flattens and standardizes source JSON data into structured parquet files. |
| Cleansed | Cleans data, generates keys, and separates facts and dimensions. |
| Integrated | Prepares conformed datasets for downstream consumption. |
| Presented | Stores curated Delta Lake tables for analytics and reporting. |

## Repository Structure

```text
ADF/
  dataset/           Azure Data Factory datasets
  factory/           ADF factory configuration
  linkedService/     ADF linked services
  pipeline/          Ingestion and processing pipelines

Databricks/
  Jobs/              PySpark jobs by data layer
  SQL/               Table and view registration scripts
  Utils/             Shared helpers, schemas, and key generation logic
  Main.py            Dynamic Databricks job entry point
  databricks.yml     Databricks Asset Bundle configuration

PBI/
  Model.pbix         Power BI reporting model

azure-pipelines.yml      CI/CD pipeline for ADF and Databricks deployment
azure-pipelines-sql.yml  CI/CD pipeline for SQL table and view deployment
```

## Data Processing Flow

1. Azure Data Factory triggers the master ingestion pipeline.
2. The source pipeline calls the job data REST API and stores raw responses in the Landed layer.
3. Databricks jobs process the data through Sourced, Cleansed, Integrated, and Presented layers.
4. PySpark transformations flatten nested job data and create fact and dimension outputs.
5. NLP logic extracts skills from job descriptions to support skill demand analytics.
6. Curated Delta tables are registered for SQL-based access and Power BI reporting.

## Analytics Use Cases

- Identify trending skills across job postings.
- Analyze remote vs on-site job availability.
- Compare employers and hiring activity.
- Explore salary ranges by job role and publisher.
- Track job benefits offered across postings.
- Build Power BI dashboards for job market insights.

## CI/CD

The project includes Azure DevOps pipelines for automated deployment:

- `azure-pipelines.yml` validates and deploys Azure Data Factory ARM templates and Databricks bundles.
- `azure-pipelines-sql.yml` deploys SQL table and view definitions to the target Databricks SQL environment.

## Project Highlights

- Built a production-style Azure data engineering workflow from ingestion to reporting.
- Implemented medallion architecture using ADLS, PySpark, parquet, and Delta Lake.
- Added NLP-based enrichment to extract skills from unstructured job descriptions.
- Automated cloud asset deployment using Azure DevOps and Databricks Asset Bundles.
- Designed datasets suitable for dimensional analytics and Power BI dashboards.

## Future Enhancements

- Add data quality checks for nulls, duplicates, schema drift, and API failures.
- Add monitoring and alerting for failed pipeline runs.
- Improve incremental loading and merge logic in Delta Lake tables.
- Expand Power BI dashboards with additional trend and geography-based analysis.
- Add unit tests for transformation helper functions.
