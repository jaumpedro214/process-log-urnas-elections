from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, TimestampType


SPARK_MASTER_URL = "spark://spark:7077"
spark = (
    SparkSession.builder
    .master(SPARK_MASTER_URL)
    .config("spark.executor.memory", "4g")
    .appName("Calculate metrics")
    .getOrCreate()
)

# Reduce number of shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", 5)
# Reduce log level
spark.sparkContext.setLogLevel("WARN")


def print_votes():
    BASE_PATH = '/data/votes'
    # METADATA_PATH = f'/data/metadata'

    df = (
        spark.read
        .format("parquet")
        .load(BASE_PATH)
    )

    df = df\
    .orderBy('log_file_name','datetime')\
    .filter( F.col('turno').isNull() )\
    .select( "turno", "uf", "log_file_name", "datetime", "operation", "zona", "secao", "vote_local_id")\
    .show(500, False)

def print_metadata():
    METADATA_PATH = f'/data/metadata'

    df = (
        spark.read
        .format("parquet")
        .load(METADATA_PATH)
    )

    df = df.filter( F.col('date') == '2022-10-30' )
    df = df.filter( F.col('log_file_name') == 'file:/data/logs/2_RN/o00407-1621700320079.csv' )

    df.show(500, False)

def print_votos_estatisticas():
    BASE_PATH = '/data/votos_estatisticas'

    df = (
        spark.read
        .format("parquet")
        .load(BASE_PATH)
    )

    df\
    .groupBy('SG_UF', 'NR_TURNO')\
    .agg( F.count('*').alias('total') )\
    .show()

    df\
    .withColumn('date', F.date_format(F.col('DT_INICIO_VOTO'), 'yyyy-MM-dd'))\
    .groupBy('date')\
    .agg( F.count('*').alias('total') )\
    .show()

    # Number of null values in each column
    df.select([F.count(F.when(F.isnull(c), c)).alias(c) for c in df.columns]).show()

if __name__ == "__main__":

    print_votos_estatisticas()
