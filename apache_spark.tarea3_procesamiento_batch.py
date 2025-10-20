# Modulos Importados
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType

# Inicializa Spark
spark = SparkSession.builder.appName('totales_hurtos_por_depto').getOrCreate()

# Ruta del CSV en HDFS
file_path = 'hdfs://localhost:9000/Tarea3_procesamiento_batch/hurtos_colombia.csv'

# Leer el CSV
df = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load(file_path)

# Detectar columnas
cols = df.columns
dept_col = next((c for c in cols if c.strip().upper() == 'DEPARTAMENTO'), None)
if dept_col is None:
    dept_col = next((c for c in cols if any(k in c.upper() for k in ['DEPART','DEPTO','DEPT'])), None)

date_col = next((c for c in cols if 'FECHA' in c.upper() or 'DATE' in c.upper()), None)
year_col = next((c for c in cols if any(k in c.upper() for k in ['AÑO','ANIO','ANO','YEAR'])), None)

if dept_col is None:
    raise RuntimeError("No se encontró columna de departamento. Columnas disponibles: " + ", ".join(cols))

# Construir columna 'year' (int)
if year_col:
    df_with_year = df.withColumn('year', F.col(year_col).cast(IntegerType()))
elif date_col:
    formats = [
        'yyyy-MM-dd HH:mm:ss', 'yyyy-MM-dd', 'yyyy/MM/dd',
        'dd/MM/yyyy HH:mm:ss', 'dd/MM/yyyy', 'dd-MM-yyyy',
        'yyyyMMdd', 'ddMMyyyy'
    ]
    ts = None
    for fmt in formats:
        ts = F.coalesce(ts, F.to_timestamp(F.col(date_col), fmt)) if ts is not None else F.to_timestamp(F.col(date_col), fmt)
    year_from_ts = F.year(ts).cast(IntegerType())
    year_from_regex_any = F.regexp_extract(F.col(date_col).cast('string'), r'(\d{4})', 1).cast(IntegerType())
    year_prefix = F.substring(F.regexp_replace(F.col(date_col).cast('string'), r'\D', ''), 1, 4).cast(IntegerType())
    df_with_year = df.withColumn('year', F.coalesce(year_from_ts, year_from_regex_any, year_prefix))
else:
    df_with_year = df

# Normalizar nombre de departamento y filtrar nulos en departamento para el conteo total
df_dept = df_with_year.select(F.trim(F.col(dept_col)).alias('Departamento'), F.col('year'))

# Calcular rango de años detectado
years_stats = df_dept.filter(F.col('year').isNotNull()) \
                     .filter((F.col('year') >= 1900) & (F.col('year') <= 2100)) \
                     .agg(F.min('year').alias('min_year'), F.max('year').alias('max_year')) \
                     .collect()

if years_stats and len(years_stats) > 0:
    min_year = years_stats[0]['min_year']
    max_year = years_stats[0]['max_year']
else:
    min_year = None
    max_year = None

# Contar cuántos registros no tienen año detectado
no_year_count = df_dept.filter(F.col('year').isNull()).count()
total_rows = df_dept.count()

# Agrupar todos los registros por departamento
agg_total = df_dept.filter(F.col('Departamento').isNotNull()) \
                   .groupBy('Departamento') \
                   .count() \
                   .orderBy(F.desc('count'))

# Recoger resultado
rows = agg_total.collect()

# Imprimir encabezado con el rango de años detectado y resumen
if min_year is not None and max_year is not None:
    print(f"\nLa siguiente tabla muestra el número total de hurtos por departamento")
    print(f"entre los años {min_year} y {max_year}.")
else:
    print("\nNo se detectaron años válidos en los registros.")
    print("La siguiente tabla muestra el número total de hurtos por departamento sobre todos los registros disponibles.")

print(f"(Total registros: {total_rows}; registros sin año detectado: {no_year_count})")
print("="*80)
print(f"{'Departamento':60} | {'Total_hurtos'}")
print("-"*80)

for r in rows:
    dept = r['Departamento']
    cnt = r['count']
    print(f"{str(dept):60} | {cnt}")

print("\nFin del listado.")

# Ruta donde se guardará el CSV
output_path = "/home/hadoop/resultados_conteo_hurtos_colombia.csv"

# Guardar como CSV con encabezado
agg_total.coalesce(1).write.mode('overwrite').option('header', 'true').csv(output_path)

print(f"\n✅ Archivo guardado en: {output_path}")
