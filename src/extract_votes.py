from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

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


def remove_unnecessary_columns( df ):
    return df.drop(
        "operation_label",
        "log_id",
        "operation_label_2",
        "operation_id",
    )

def isolate_votes(df):
    """
    Isolate the votes from the logs, assigning a unique ID to each vote.
    The id is unique only within the log file.

    Args:
        df (Spark DataFrame): DataFrame with the logs

    Returns:
        Spark DataFrame: DataFrame with the votes isolated
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
    MARKER_OPERATION = OPERATIONS[0]

    # Assign a unique id to each possible vote in a session
    df = (
        df
        .filter(
            F.col("operation").isin(OPERATIONS)
        )
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
    )


    # Some rules to helping filter out invalid votes
    # and remove processing errors

    # 1. Make sure that each vote
    #    has exactly one operation of 'O voto do eleitor foi computado'
    df = (
        df
        .withColumn(
            "in_voto_computado",
            F.when(
                F.col("operation") == 'O voto do eleitor foi computado',
                F.lit(1)
            ).otherwise(F.lit(0))
        )
        .withColumn(
            "vote_count",
            F.sum("in_voto_computado").over(
                Window
                .partitionBy("turno", "log_file_name", "vote_local_id")
            )
        )
        .filter(
            F.col("vote_count") == 1
        )
    )

    # Remove columns that are not necessary anymore
    df = df.drop(
        "in_voto_computado",
        "vote_count",
        "maker"
    )

    return df

def add_metadata(df):

    # Filter only the metadata operations
    regex_patterns = {
        "modelo_urna": "Modelo de Urna: (.*)",
        "municipio": "Município: ([0-9]+)",
        "zona": "Zona Eleitoral: ([0-9]+)",
        "secao": "Seção Eleitoral: ([0-9]+)",
        "turno": "Turno da UE: ([0-9]+)",
    }

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
            ).otherwise( F.lit(None) )
        )


    # Propagate the metadata to the votes
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

    df_logs = df_logs.filter( F.col('operation_label_2').isin(['VOTA', 'GAP']) )

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
        .save(f"/data/votes/")



