from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

from pyspark.sql.window import Window


SPARK_MASTER_URL = "spark://spark:7077"

LOG_SCHEMA = StructType([

    StructField("turno", StringType(), True),
    StructField("uf", StringType(), True),
    StructField("log_file_name", StringType(), True),

    StructField("datetime", TimestampType(), True),
    StructField("operation_label", StringType(), True),
    StructField("some_id", StringType(), True),
    StructField("operation_label_2", StringType(), True),
    StructField("operation", StringType(), True),
    StructField("operation_id", StringType(), True),

])

METADATA_SCHEMA = StructType([
    
    StructField("turno", StringType(), True),
    StructField("uf", StringType(), True),
    StructField("zona", StringType(), True),
    StructField("secao", StringType(), True),

    StructField("log_file_name", StringType(), True),
    StructField("modelo_urna", StringType(), True),
    StructField("municipio", StringType(), True),

])

spark = (
    SparkSession.builder
    .master(SPARK_MASTER_URL)
    .config("spark.executor.memory", "4g")
    .appName("Extract votes from logs")
    .getOrCreate()
)
# Reduce number of shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", 5)
# Reduce log level
spark.sparkContext.setLogLevel("WARN")


def read_logs_with_metadata( logs_path, metadata_path ):

    # The log file name will serve as the session id
    df_logs = (
        spark
        .read.format("parquet")
        .schema(LOG_SCHEMA)
        .load(logs_path)
    )

    df_metadata = (
        spark
        .read.format("parquet")
        .schema(METADATA_SCHEMA)
        .load(metadata_path)
    )

    # Join the logs with the metadata
    # on turno, uf, log_file_name

    df_full_logs = (
        df_logs
        .join(
            df_metadata,
            on=[
                "turno", "uf", "log_file_name"
            ],
            how='left'
        )
    )

    return df_full_logs

if __name__ == "__main__":

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
    MINIMAL_OPERATION_COUNT = 4

    BASE_LOGS_PATH = '/data/parquet/*'
    METADATA_PATH = '/data/session_metadata/'
    df_full_logs = read_logs_with_metadata(BASE_LOGS_PATH, METADATA_PATH)

    # Assign a unique id to each vote in a session
    df_full_logs = (
        df_full_logs
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
                .partitionBy("turno", "uf", "zona", "secao")
                .orderBy("datetime")
            )
        )
        .withColumn(
            "number_of_operations_in_vote",
            F.count("operation").over(
                Window
                .partitionBy("turno", "uf", "zona", "secao", "vote_local_id")
            )
        )
        .filter(
            F.col("number_of_operations_in_vote") >= MINIMAL_OPERATION_COUNT
        )
    )

    # Remove columns that are not needed
    df_full_logs = (
        df_full_logs
        .drop("maker")
        .drop("number_of_operations_in_vote")
        .drop("log_file_name")
    )


    # Save the votes
    df_full_logs\
        .write.format("parquet")\
        .mode("overwrite")\
        .partitionBy("turno", "uf", "zona")\
        .save("/data/votes/")


