from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType)

from pyspark.sql.window import Window


SPARK_MASTER_URL = "spark://spark:7077"

spark = (
    SparkSession.builder
    .master(SPARK_MASTER_URL)
    .config("spark.executor.memory", "4g")
    .appName("Extract votes and add metadata")
    .getOrCreate()
)
# Reduce number of shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", 5)
# Reduce log level
spark.sparkContext.setLogLevel("WARN")


def remove_unnecessary_columns(df):
    """
    Remove unnecessary columns from the DataFrame.

    Args:
        df (
            spark.sql.dataframe.DataFrame
        ): DataFrame with the logs

    Returns:
        spark.sql.dataframe.DataFrame: DataFrame with the unnecessary columns removed
    """
    return df.drop(
        "operation_label",
        "log_id",
        "operation_label_2",
        "operation_id",
    )


def isolate_votes(df):
    """
    Isolate the votes from the logs, assigning a sequential
    id to each vote of a session.
    id = 1 is the first vote of the session, id = 2 is the second vote, etc.
    The id is unique only within the log file.


    Args:
        df (
            spark.sql.dataframe.DataFrame
        ): DataFrame with the logs

    Returns:
        spark.sql.dataframe.DataFrame: DataFrame with the votes isolated
    """

    OPERATIONS = [
        'Aguardando digitação do título',
        'Título digitado pelo mesário',
        'Eleitor foi habilitado',
        'O voto do eleitor foi computado',

        # Biometria
        'Solicita digital. Tentativa [1] de [4]',
        'Solicita digital. Tentativa [2] de [4]',
        'Solicita digital. Tentativa [3] de [4]',
        'Solicita digital. Tentativa [4] de [4]',
        'Solicitação de dado pessoal do eleitor para habilitação manual'
    ]
    MARKER_OPERATION = OPERATIONS[3]

    # Filter only the operations that are related to votes
    df = (
        df
        .filter(
            F.col("operation").isin(OPERATIONS)
        )
    )

    # Assign a unique id to each possible vote in a session
    df = (
        df
        .withColumn(
            "maker",
            F.when(
                F.col("operation") == MARKER_OPERATION,
                F.lit(1)
            ).otherwise(F.lit(0))
        )
        .withColumn(
            "vote_local_id",
            F.sum("maker").over(
                Window
                .partitionBy("uf", "log_file_name")
                .orderBy("datetime")
            )
        )
        # Subtract 1 from vote_local_id whete operation == MARKER_OPERATION
        # This is necessary because the marker operation is the last operation
        # and we want it to have the same id as the preceeding operations
        .withColumn(
            "vote_local_id",
            F.when(
                F.col("operation") == MARKER_OPERATION,
                F.col("vote_local_id") - 1
            ).otherwise(F.col("vote_local_id"))
        )
    )

    # TODO: THIS SHOULD BECAME A TEST
    # Some rules to helping filter out invalid votes
    # and remove processing errors

    # 1. Make sure that each vote
    #    has exactly one operation of 'O voto do eleitor foi computado'
    # df = (
    #     df
    #     .withColumn(
    #         "in_voto_computado",
    #         F.when(
    #             F.col("operation") == 'O voto do eleitor foi computado',
    #             F.lit(1)
    #         ).otherwise(F.lit(0))
    #     )
    #     .withColumn(
    #         "vote_count",
    #         F.sum("in_voto_computado").over(
    #             Window
    #             .partitionBy("turno", "log_file_name", "vote_local_id")
    #         )
    #     )
    #     .filter(
    #         F.col("vote_count") == 1
    #     )
    # )

    # Remove columns that are not necessary anymore
    df = df.drop(
        "in_voto_computado",
        "vote_count",
        "maker"
    )

    return df


def add_metadata(df):
    """
    Add metadata to the votes DataFrame. 
    The metadata is extracted using regex from the operation column and
    propagated using the datetime column.
    The following metadata is added: MODELO_URNA, TURNO, MUNICIPIO, ZONA, SECAO

    Args:
        df (
        spark.sql.dataframe.DataFrame
        ): DataFrame with the votes

    Returns:
        spark.sql.dataframe.DataFrame: DataFrame with the votes and metadata
    """

    # These are the patterns that will be used to extract the metadata
    regex_patterns = {
        "modelo_urna": "Modelo de Urna: (.*)",
        "municipio": "Município: ([0-9]+)",
        "zona": "Zona Eleitoral: ([0-9]+)",
        "secao": "Seção Eleitoral: ([0-9]+)",
        "turno": "Turno da UE: ([0-9]+)",
    }

    # Thes are the keywords that will be used to filter the logs
    # and only extract the metadata from the lines that contain them
    filter_patterns = {
        "modelo_urna": "Modelo de Urna",
        "municipio": "Município",
        "zona": "Zona",
        "secao": "Seção Eleitoral",
        "turno": "Turno",
    }

    # Add a column for each metadata
    for metadata, pattern in regex_patterns.items():
        df = df.withColumn(
            metadata,
            # Try to extract the metadata from the operation column
            # return null if the pattern is not found
            F.when(
                F.col("operation").contains(filter_patterns[metadata]),
                F.regexp_extract(F.col("operation"), pattern, 1)
            ).otherwise(F.lit(None))
        )

    # Fill the null values with the last non-null value
    # according to the partition (uf, log_file_name) and order (datetime)
    for metadata in regex_patterns.keys():
        df = df.withColumn(
            metadata,
            F.last(metadata, True).over(
                Window
                .partitionBy("uf", "log_file_name")
                .orderBy("datetime")
            )
        )

    return df


if __name__ == "__main__":

    SCHEMA = StructType([
        StructField("datetime", TimestampType(), True),
        StructField("operation_label", StringType(), True),
        StructField("operation_label_2", StringType(), True),
        StructField("operation", StringType(), True),
        StructField("operation_id", StringType(), True),
        StructField("uf", StringType(), True),
        StructField("turno", StringType(), True),
        StructField("log_file_name", StringType(), True),
    ])

    BASE_PATH = '/data/parquet'
    df_logs = spark\
        .read.format("parquet")\
        .option("encoding", "ISO-8859-1")\
        .schema(SCHEMA)\
        .load(f"{BASE_PATH}/*")\
        # .limit(100000)

    # Filter only the operations that are related to votes
    df_logs = df_logs.filter(
        F.col('operation_label_2').isin(['VOTA', 'GAP'])
    )

    df_logs = add_metadata(df_logs)
    df_logs = isolate_votes(df_logs)
    df_logs = remove_unnecessary_columns(df_logs)

    # df_logs.show(100, False)

    # Save the result
    df_logs\
        .write\
        .format("parquet")\
        .partitionBy("turno", "uf", "zona")\
        .mode("overwrite")\
        .save("/data/votes/")
