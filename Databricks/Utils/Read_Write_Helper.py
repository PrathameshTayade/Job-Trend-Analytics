from pyspark.sql import DataFrame, SparkSession

ADLS_CONTAINER = "lakeprod"
ADLS_ACCOUNT = "stajobtrendanalytics"

SOURCED_FOLDER = 'Sourced'
CLEANSED_FOLDER = "Cleansed"
INTEGRATED_FOLDER = "Integrated"
PRESENTED_FOLDER = "Presented"


def get_adls() -> str:
    return f"abfss://{ADLS_CONTAINER}@{ADLS_ACCOUNT}.dfs.core.windows.net/"


def _join_path(*parts: str) -> str:
    base = get_adls().rstrip("/")
    segments = [part.strip("/") for part in parts if part]
    return "/".join([base, *segments])


def get_sourced_path(*relative_path: str) -> str:
    return _join_path(SOURCED_FOLDER, *relative_path)

def get_cleansed_path(*relative_path: str) -> str:
    return _join_path(CLEANSED_FOLDER, *relative_path)


def get_integrated_path(*relative_path: str) -> str:
    return _join_path(INTEGRATED_FOLDER, *relative_path)


def get_presented_path(*relative_path: str) -> str:
    return _join_path(PRESENTED_FOLDER, *relative_path)


def _path_from_run(layer_folder: str, relative_path: str, run) -> str:
    if run is None:
        return _join_path(layer_folder, relative_path) if relative_path else _join_path(layer_folder)

    parts = [layer_folder]
    if getattr(run, "dataprovider", None):
        parts.append(run.dataprovider)
    if getattr(run, "slot", None):
        parts.append(run.slot)
    if getattr(run, "submission", None):
        parts.append(run.submission)
    if relative_path:
        parts.append(relative_path)
    return _join_path(*parts)


def read_parquet(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.parquet(path)


def read_parquet_sourced(
    spark: SparkSession,
    relative_path: str = "",
    run=None,
) -> DataFrame:
    path = _path_from_run(SOURCED_FOLDER, relative_path, run)
    return read_parquet(spark, path)

def read_parquet_cleansed(
    spark: SparkSession,
    relative_path: str = "",
    run=None,
) -> DataFrame:
    path = _path_from_run(CLEANSED_FOLDER, relative_path, run)
    return read_parquet(spark, path)


def read_parquet_integrated(
    spark: SparkSession,
    relative_path: str = "",
    run=None,
) -> DataFrame:
    path = _path_from_run(INTEGRATED_FOLDER, relative_path, run)
    return read_parquet(spark, path)


def read_parquet_presented(
    spark: SparkSession,
    relative_path: str = "",
    run=None,
) -> DataFrame:
    path = _path_from_run(PRESENTED_FOLDER, relative_path, run)
    return read_parquet(spark, path)


def write_parquet(
    df: DataFrame,
    path: str,
    mode: str = "overwrite",
) -> None:
    df.write.mode(mode).parquet(path)


def write_parquet_cleansed(
    df: DataFrame,
    relative_path: str = "",
    run=None,
    mode: str = "overwrite",
) -> None:
    path = _path_from_run(CLEANSED_FOLDER, relative_path, run)
    write_parquet(df, path, mode)


def write_parquet_integrated(
    df: DataFrame,
    relative_path: str = "",
    run=None,
    mode: str = "overwrite",
) -> None:
    path = _path_from_run(INTEGRATED_FOLDER, relative_path, run)
    write_parquet(df, path, mode)


def write_parquet_presented(
    df: DataFrame,
    relative_path: str = "",
    run=None,
    mode: str = "overwrite",
) -> None:
    path = _path_from_run(PRESENTED_FOLDER, relative_path, run)
    write_parquet(df, path, mode)
