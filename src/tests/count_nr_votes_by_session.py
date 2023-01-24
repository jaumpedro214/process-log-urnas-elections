from pyspark.sql import SparkSession
import pyspark.sql.functions as F

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


def count_nr_votes_by_session():

    df = (
        spark.read
        .format("parquet")
        .load("/data/votos_estatisticas/*")
    )

    df = (
        df
        .select(
            "NR_TURNO", "SG_UF", "NR_ZONA", "NR_SECAO", "NR_SEQ_VOTO"
        )
    )

    # Count the number of votes in each session in each turn

    df = (
        df
        .groupBy("NR_TURNO", "SG_UF", "NR_ZONA", "NR_SECAO")
        .agg(F.count("NR_SEQ_VOTO").alias("count"))
    )

    # The number should not exceed 500

    count = (
        df
        .filter(F.col("count") > 500)
        .count()
    )

    if count == 0:
        print("\n\nOK\n\n")
    else:
        print("\n\nFAIL\n\n")
