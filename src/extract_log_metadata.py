from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import os

SPARK_MASTER_URL = "spark://spark:7077"
SCHEMA = StructType([
    StructField("datetime", TimestampType(), True),
    StructField("operation_label", StringType(), True),
    StructField("log_id", StringType(), True),
    StructField("operation_label_2", StringType(), True),
    StructField("operation", StringType(), True),
    StructField("operation_id", StringType(), True),
    StructField("uf", StringType(), True),
    StructField("turno", StringType(), True),
    StructField("log_file_name", StringType(), True),
])
BASE_PATH = '/data/parquet'

spark = (
    SparkSession.builder
    .master(SPARK_MASTER_URL)
    .config("spark.executor.memory", "4g")
    .appName("Extract logs metadata")
    .getOrCreate()
)
# Reduce number of shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", 5)
# Reduce log level
spark.sparkContext.setLogLevel("WARN")

def extract_metadata_from_log( file ):
    """
    Extract metadata from the log file and create a temporary view
    with the informations about each session.

    Args:
        file (str or list): parquet file path or list of parquet files
    """    
    
    df_logs = spark\
        .read.format("parquet")\
        .option("encoding", "ISO-8859-1")\
        .schema(SCHEMA)\
        .load(file)\
        
    df_metadata = (
        df_logs
        .filter(
            F.col("operation_label_2") == "GAP"
        )
        .filter(
            F.col("operation").contains("Modelo de Urna")
            | F.col("operation").contains("Município")
            | F.col("operation").contains("Zona")
            | F.col("operation").contains("Seção")
        )
    )

    regex_patterns = {
        "modelo_urna": "Modelo de Urna: (.*)",
        "municipio": "Município: ([0-9]+)",
        "zona": "Zona Eleitoral: ([0-9]+)",
        "secao": "Seção Eleitoral: ([0-9]+)",
    }

    for key, value in regex_patterns.items():
        df_metadata = df_metadata.withColumn(key, F.regexp_extract(F.col("operation"), value, 1))


    # Get non-null values of each column
    df_metadata = (
        df_metadata
        .groupBy("turno", "uf", "log_file_name")
        .agg(
            F.max("modelo_urna").alias("modelo_urna"),
            F.max("municipio").alias("municipio"),
            F.max("zona").alias("zona"),
            F.max("secao").alias("secao"),
        )
    )

    df_metadata.show(30, False)

    return df_metadata


if __name__ == "__main__":

    # Get metadata from all logs and create a dataframe
    df_metadata = extract_metadata_from_log(
        os.path.join(BASE_PATH, "*")
    )

    # Save metadata to parquet
    df_metadata\
        .write.format("parquet")\
        .mode("overwrite")\
        .partitionBy("turno", "uf", "zona")\
        .save(
            os.path.join(BASE_PATH, "../session_metadata")
        )

