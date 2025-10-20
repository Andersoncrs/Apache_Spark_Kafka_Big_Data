from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, min as smin, max as smax, count, to_timestamp, from_unixtime
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, LongType

# Crear SparkSession
spark = SparkSession.builder \
    .appName("KafkaSparkConsumer_SensorES_simple") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Esquema
schema = StructType([
    StructField("id_sensor", IntegerType(), True),
    StructField("presion", DoubleType(), True),
    StructField("nivel_bateria", IntegerType(), True),
    StructField("velocidad_viento", DoubleType(), True),
    StructField("fecha", LongType(), True)   # epoch en segundos
])

# Leer stream desde Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor_data") \
    .option("startingOffsets", "latest") \
    .load()

# Parsear JSON
parsed = df_kafka.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Crear columna timestamp a partir de fecha
parsed_ts = parsed.withColumn("ts", to_timestamp(from_unixtime(col("fecha"))))

# Agrupar por ventana de 10 segundos 
# Calcular promedio, min, max y conteo
stats_ventana = parsed_ts.groupBy(
    window(col("ts"), "10 seconds"),
    col("id_sensor")
).agg(
    avg(col("presion")).alias("presion_promedio"),
    smin(col("presion")).alias("presion_min"),
    smax(col("presion")).alias("presion_max"),
    avg(col("nivel_bateria")).alias("bateria_promedio"),
    smin(col("nivel_bateria")).alias("bateria_min"),
    smax(col("nivel_bateria")).alias("bateria_max"),
    avg(col("velocidad_viento")).alias("velocidad_promedio"),
    smin(col("velocidad_viento")).alias("velocidad_min"),
    smax(col("velocidad_viento")).alias("velocidad_max"),
    count("*").alias("conteo_mensajes")
)

# Escribir resultados en la consola
query = stats_ventana.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
