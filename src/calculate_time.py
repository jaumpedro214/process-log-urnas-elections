from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, LongType,
    ByteType, IntegerType, BooleanType
)
from pyspark.sql.window import Window


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


def extract_delta_time(df, time_col, partition_cols):
    """
    Create a column with the time difference between the current row and the previous one.
    In practice, this is the time between the current operation and the previous one.
    """

    time_window = Window.partitionBy(partition_cols).orderBy(time_col)
    return (
        df
        .withColumn(
            "delta_time",
            F.col(time_col).cast("long")
            - F.lag(time_col).over(time_window).cast("long")
        )
    )


def add_operation_type_label(df):
    """Add a column with a label for each operation type

    Args:
        df (Spark DataFrame): DataFrame with the operations

    Returns:
        Spark DataFrame: DataFrame with the operations and the label column
    """

    OPERATIONS = [
        ('DIGITACAO', [
            'Aguardando digitação do título',
            'Título digitado pelo mesário',
        ]),
        ('BIOMETRIA', [
            'Solicita digital. Tentativa [1] de [4]',
            'Solicita digital. Tentativa [2] de [4]',
            'Solicita digital. Tentativa [3] de [4]',
            'Solicita digital. Tentativa [4] de [4]',
            'Solicitação de dado pessoal do eleitor para habilitação manual',
            'Eleitor foi habilitado',
        ]),
        ('VOTO', [
            'O voto do eleitor foi computado',
        ])
    ]

    # Create a when condition for each operation

    # IF operation in list1 THEN label1
    # IF operation in list2 THEN label2
    # ...
    # ELSE labelN

    when_clause = F.when(
        F.col("operation").isin(OPERATIONS[0][1]), OPERATIONS[0][0]
    )
    for operation in OPERATIONS[1:]:
        when_clause = when_clause.when(
            F.col("operation").isin(operation[1]), operation[0]
        )
    when_clause = when_clause.otherwise("OTHER")

    return (
        df
        .withColumn("operation_type", when_clause)
    )


if __name__ == "__main__":

    BASE_URL = '/data/votes/'
    SCHEMA = StructType([
        StructField("turno", StringType(), True),
        StructField("uf", StringType(), True),
        StructField("zona", StringType(), True),
        StructField("secao", StringType(), True),
        StructField("vote_local_id", LongType(), True),

        StructField("datetime", TimestampType(), True),
        StructField("operation", StringType(), True),
        StructField("modelo_urna", StringType(), True),
        StructField("municipio", StringType(), True),
    ])

    df_votos = spark\
        .read.format("parquet")\
        .schema(SCHEMA)\
        .load(BASE_URL)\

    df_votos = extract_delta_time(
        df_votos, "datetime", 
        ["turno", "uf", "zona", "secao", "vote_local_id"]
    )
    df_votos = add_operation_type_label(df_votos)

    OPERATIONS_TYPES = ['DIGITACAO', 'BIOMETRIA', 'VOTO']
    BIOMETRIA_TENTATIVAS = [
        'Solicita digital. Tentativa [1] de [4]',
        'Solicita digital. Tentativa [2] de [4]',
        'Solicita digital. Tentativa [3] de [4]',
        'Solicita digital. Tentativa [4] de [4]',
        'Solicitação de dado pessoal do eleitor para habilitação manual'
    ]

    df_votos = (
        df_votos
        .groupBy("turno", "uf", "zona", "secao", "vote_local_id")
        .agg(
            # Metrics
            # Number of operations in BIOMETRIA
            F.count(
                F.when(F.col("operation").like("Solicita%"), 1)
            ).alias('tentativas_biometria'),

            # Success biometria
            F.min(
                F.when(
                    F.col(
                        "operation") == "Solicitação de dado pessoal do eleitor para habilitação manual", 0
                ).otherwise(1)
            ).alias('sucesso_biometria'),

            # Sum delta time for each operation type
            *[
                F.sum(
                    F.when(F.col("operation_type") ==
                           operation_type, F.col("delta_time"))
                ).alias(f"tempo_{operation_type.lower()}")
                for operation_type in OPERATIONS_TYPES
            ],

            # Start and end time
            F.min(F.col("datetime")).alias("inicio_voto"),
            F.max(F.col("datetime")).alias("fim_voto"),

            # Metadata
            F.first(F.col("modelo_urna"), True).alias("modelo_urna"),
            F.first(F.col("municipio"), True).alias("municipio"),
        )
        # make sucesso_biometria NULL if there is no biometria
        .withColumn(
            "sucesso_biometria",
            F.when(F.col("tentativas_biometria") == 0,
                   None).otherwise(F.col("sucesso_biometria"))
        )
        .withColumn(
            "tempo_total",
            F.col("tempo_digitacao") + \
            F.col("tempo_biometria") + F.col("tempo_voto")
        )
    )

    # Rename columns, change types and save to parquet

    df_votos = (
        df_votos
        .withColumnRenamed("vote_local_id", "NR_SEQ_VOTO")
        .withColumnRenamed("turno", "NR_TURNO")
        .withColumnRenamed("uf", "SG_UF")
        .withColumnRenamed("zona", "NR_ZONA")
        .withColumnRenamed("secao", "NR_SECAO")
        .withColumnRenamed("modelo_urna", "DS_MODELO_URNA")
        .withColumnRenamed("municipio", "NR_MUNICIPIO")
        .withColumnRenamed("inicio_voto", "DT_INICIO_VOTO")
        .withColumnRenamed("fim_voto", "DT_FIM_VOTO")
        .withColumnRenamed("tentativas_biometria", "NR_TENTATIVAS_BIOMETRIA")
        .withColumnRenamed("sucesso_biometria", "IND_SUCESSO_BIOMETRIA")
        .withColumnRenamed("tempo_digitacao", "TEMPO_DIGITACAO")
        .withColumnRenamed("tempo_biometria", "TEMPO_BIOMETRIA")
        .withColumnRenamed("tempo_voto", "TEMPO_VOTO")
        .withColumnRenamed("tempo_total", "TEMPO_TOTAL")
        # Change types
        # Turno to ByteType (smallint)
        .withColumn("NR_TURNO", F.col("NR_TURNO").cast(ByteType()))
        # sucesso_biometria to boolean
        .withColumn("IND_SUCESSO_BIOMETRIA", F.col("IND_SUCESSO_BIOMETRIA").cast(BooleanType()))
        # tempo_* to IntegerType (int)
        .withColumn("TEMPO_DIGITACAO", F.col("TEMPO_DIGITACAO").cast(IntegerType()))
        .withColumn("TEMPO_BIOMETRIA", F.col("TEMPO_BIOMETRIA").cast(IntegerType()))
        .withColumn("TEMPO_VOTO",      F.col("TEMPO_VOTO").cast(IntegerType()))
        .withColumn("TEMPO_TOTAL",     F.col("TEMPO_TOTAL").cast(IntegerType()))
    )

    # df_votos.show(25, True)
    (
        df_votos
        .write.format("parquet")
        .mode("overwrite")
        # .partitionBy("NR_TURNO", "SG_UF", "NR_ZONA")
        .save("/data/votos_estatisticas/")
    )
