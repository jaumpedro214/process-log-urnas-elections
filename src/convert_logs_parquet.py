from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType
)
import os

SPARK_MASTER_URL = "spark://spark:7077"
SCHEMA = StructType([
    StructField("datetime", TimestampType(), True),
    StructField("operation_label", StringType(), True),
    StructField("some_id", StringType(), True),
    StructField("operation_label_2", StringType(), True),
    StructField("operation", StringType(), True),
    StructField("operation_id", StringType(), True),
])
BASE_PATH = '/data/logs'

spark = (
    SparkSession.builder
    .master(SPARK_MASTER_URL)
    .config("spark.executor.memory", "4g")
    .appName("Convert logs to parquet")
    .getOrCreate()
)
# Reduce number of shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", 5)
# Reduce log level
spark.sparkContext.setLogLevel("WARN")


def process_logs_from_dir(directory):
    """
    Process all logs from a directory

    Parameters
    ----------
    dir : str
        Directory path, must end with 
    """

    dir_name = directory.split('/')[-1]
    turno, uf = dir_name.split('_')

    df_logs = (
        spark
        .read
        .schema(SCHEMA)
        .option("delimiter", "\t")
        .option("header", "false")
        .option("inferSchema", "false")
        .option("timestampFormat", "dd/MM/yyyy HH:mm:ss")
        .option("encoding", "ISO-8859-1")
        .csv(f'{directory}/*.csv')
    )

    # Add UF and Turno columns
    df_logs = df_logs.withColumn('uf', F.lit(uf))
    df_logs = df_logs.withColumn('turno', F.lit(turno))
    # Add input file name column
    df_logs = df_logs.withColumn('log_file_name', F.input_file_name())

    # Convert to parquet
    df_logs\
        .write\
        .mode("overwrite")\
        .option("encoding", "ISO-8859-1")\
        .parquet(f'/data/parquet/{dir_name}')


def process_all_logs():
    folders = os.listdir(BASE_PATH)
    for folder in folders:

        try:
            print(f'\nProcessing {folder}\n')
            process_logs_from_dir(f'{BASE_PATH}/{folder}')
        except Exception as ex:
            print(f'Error processing {folder}: {ex}')


if __name__ == "__main__":
    process_all_logs()

# Run the script
