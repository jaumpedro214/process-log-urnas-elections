from pyspark.sql import SparkSession
import os

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


def print_sample():

    BASE_PATH = '/data/parquet/'
    filepaths = [f'{BASE_PATH}{f}' for f in os.listdir(BASE_PATH)]

    df = (
        spark.read
        .format("parquet")
        .load(filepaths)
        .sample(False, 0.1)
        .limit(100)
    )

    df.show(500, False)


if __name__ == "__main__":
    print_sample()
