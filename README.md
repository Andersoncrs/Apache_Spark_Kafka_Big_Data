# Big Data: Análisis de Datos con Apache Spark y Kafka

Este repositorio contiene dos actividades principales que demuestran el uso de tecnologías de Big Data como Apache Spark y Apache Kafka para el procesamiento de datos tanto en modo batch como en tiempo real (streaming).

---

## Actividad 1: Procesamiento Batch con Apache Spark

En esta primera actividad, se realiza un análisis de datos en modo batch utilizando Apache Spark sobre un conjunto de datos de hurtos de vehículos en Colombia.

### 1.1 Descripción del Problema

La seguridad de los ciudadanos es una preocupación primordial en Colombia, en especial el hurto a vehículos que se ha consolidado como un problema importante que afecta tanto el patrimonio individual como la percepción general que hay de la seguridad en el País. Este delito no solo genera grandes daños económicos sino que también alimenta las estructuras criminales, impactando negativamente la vida de los Ciudadanos dentro del Territorio Nacional.

A pesar de los esfuerzos de las autoridades, el fenómeno ha persistido durante décadas y ha evolucionado constantemente, lo que hace que resulte esencial comprender en profundidad, con base en datos, el comportamiento de este fenómeno que impacta negativamente la calidad de vida de los ciudadanos.

Esta problemática debe ser afrontada a través de diversos análisis que nos permitan obtener un panorama general de este fenómeno, pudiendo diseñar e implementar políticas de seguridad y estrategias de prevención efectivas hacia el Hurto a Vehículos.

### 1.2 Descripción del Conjunto de Datos

El conjunto de datos seleccionado proviene de la plataforma de **Datos Abiertos de Colombia** y se titula **"HURTO A VEHÍCULOS"**. Este dataset es alimentado periódicamente con nuevos registros, lo que garantiza su vigencia y relevancia a lo largo del tiempo.

- **Fuente:** [HURTO A VEHÍCULOS - Datos Abiertos Colombia](https://www.datos.gov.co/Seguridad-y-Defensa/HURTO-A-VEH-CULOS/csb4-y6v2/about_data)
- **Contenido:** El conjunto de datos contiene la sumatoria del hurto de automotores y motocicletas a nivel nacional, medido en unidades. Cada registro representa un incidente de hurto e incluye detalles como el departamento, municipio y fecha del suceso.

### 1.3 Tutorial de Ejecución del Script `tarea3_procesamiento_batch.py`

Este script de PySpark está diseñado para leer el conjunto de datos de hurtos, procesarlo y generar un resumen del total de hurtos por departamento.

#### Prerrequisitos

- Una máquina (física o virtual, por ejemplo, con Ubuntu) con el siguiente software instalado:
  - **Apache Hadoop:** Necesario para utilizar el sistema de archivos distribuido HDFS.
  - **Apache Spark:** El motor de procesamiento que ejecutará nuestro análisis.
  - **Python 3:** El lenguaje en el que está escrito el script.

#### Pasos de Ejecución

1.  **Iniciar Hadoop:** Antes de ejecutar el script de Spark, es fundamental que los servicios de Hadoop estén activos. Para ello, ejecuta el siguiente comando en tu terminal:
    ```bash
    start-all.sh
    ```
    Este comando inicia el NameNode, DataNode y otros nucleos necesarios de HDFS y YARN.

2.  **Cargar el Dataset a HDFS:** Asegúrate de que el archivo `hurtos_colombia.csv` esté en la ruta de HDFS que espera el script: `hdfs://localhost:9000/Tarea3_procesamiento_batch/`.

3.  **Ejecutar el Script de Spark:** Una vez que Hadoop está corriendo, puedes lanzar el trabajo de Spark con el siguiente comando:
    ```bash
    python3 tarea3_procesamiento_batch.py
    ```

#### Explicación Detallada del Código (`tarea3_procesamiento_batch.py`)

El script realiza un proceso ETL (Extracción, Transformación y Carga) de forma robusta y automatizada. A continuación, se detalla cada fase:

  - Se crea una `SparkSession`, que es el punto de entrada para programar con la API de DataFrame y Dataset de Spark. Se le asigna el nombre `totales_hurtos_por_depto`.
  - Se lee el archivo `hurtos_colombia.csv` desde la ruta especificada en HDFS.
  - Se utilizan las opciones `option('header', 'true')` para indicar que la primera fila es el encabezado y `option('inferSchema', 'true')` para que Spark infiera los tipos de datos de cada columna.
  - **Detección Dinámica de Columnas:** El código bsuca el nombre de la columna de departamento, ya sea `DEPARTAMENTO`, `DEPTO`, y la columna de fecha (buscando `FECHA`, `AÑO`, etc.), esto hace que el script sea resistente a cambios en el esquema del dataset.
  - **Extracción del Año:** Se crea una columna `year`. Si existe una columna de año, la convierte a tipo entero, en caso de que no, intenta extraer el año a partir de una columna de fecha, probando múltiples formatos, esto asegura que asi esten en diferente formato las fechas, puedan ser extraidas.
  - **Limpieza de Datos:** Se normaliza el nombre del departamento eliminando espacios en blanco al inicio y al final.
  - **Análisis y Agregación:**
      - Se calcula el rango de años (mínimo y máximo) presente en los datos.
      - El núcleo del análisis consiste en agrupar todos los registros por la columna `Departamento`.
      - Luego, se aplica la función `count()` para contar el número de registros de hurtos en cada grupo.
      - Finalmente, el resultado se ordena de forma descendente para mostrar los departamentos con más hurtos primero.

4.  **Carga (Presentación y Guardado de Resultados):**
    - **Impresión en Consola:** Se imprime una tabla formateada directamente en la terminal, mostrando cada departamento y su total de hurtos, el encabezado de la tabla informa sobre el rango de años analizado y el número total de registros procesados.
    - **Guardado en Archivo:** El DataFrame agregado se guarda como un nuevo archivo CSV en la ruta local `/home/hadoop/resultados_conteo_hurtos_colombia.csv`.

### 1.4 Solución y Análisis de Resultados

Al ejecutar el script, se obtiene una tabla que clasifica los departamentos de Colombia según el número total de hurtos de vehículos registrados.

![](https://raw.githubusercontent.com/Andersoncrs/Apache_Spark_Kafka_Big_Data/refs/heads/main/apache_spark/ResultadoTablaSpark.png)


**Análisis:** El análisis de los 367,125 registros de hurtos de vehículos revela una marcada concentración geográfica del delito. Los departamentos de Antioquia y Valle del Cauca emergen como los epicentros de esta problemática, acumulando juntos más del 27% del total de los casos a nivel nacional. Esta desproporción subraya que el hurto de vehículos no es un fenómeno homogéneo, sino uno fuertemente ligado a grandes centros urbanos que funcionan como nodos económicos y logísticos, ofreciendo un amplio parque automotor y redes consolidadas para la comercialización de vehículos y autopartes robadas.

Más allá de los principales focos, se observa un segundo patrón en departamentos estratégicos como Cauca y Nariño, cuyas altas cifras se explican por su rol como corredores para economías ilegales y su proximidad a fronteras, lo que facilita el contrabando. En contraste, la baja incidencia en regiones como Vaupés o Amazonas evidencia que la densidad poblacional y el aislamiento geográfico son factores determinantes que inhiben este delito. El posicionamiento de Bogotá D.C. en un sexto lugar, por debajo de lo esperado para su tamaño, sugiere la posible efectividad de políticas de seguridad locales o diferencias en las dinámicas criminales que merecen un estudio más profundo.

Factores Clave:
- Grandes Centros Urbanos: La alta densidad de población y vehículos (ej. Medellín, Cali) crea un "mercado" de objetivos para los delincuentes.
- Estructuras Criminales Organizadas: Bandas especializadas en el hurto, desguace y venta operan principalmente en los departamentos con mayores cifras.
- Corredores Estratégicos y Fronteras: Zonas como Cauca y Nariño son clave para el contrabando y la logística de grupos ilegales.
- Aislamiento Geográfico: Departamentos con baja conectividad terrestre (ej. Vaupés) presentan las cifras más bajas al no ser prácticos para este delito.

---

## Actividad 2: Streaming de Datos con Spark y Kafka

En esta segunda actividad, se implementa una arquitectura de procesamiento de datos en tiempo real para simular y analizar datos de sensores.

### 2.1 Contextualización

Utilizaremos Apache Kafka como un sistema de mensajería distribuido que actúa como intermediario entre un productor de datos y un consumidor.

- **Productor:** El acrhivo `kafka_sensor_productor.py` es un script de Python que simula un sensor, el cual genera datos aleatorios como la presión, nivel de batería, velocidad del viento cada segundo y los envía a un topic llamado `sensor_data`.
- **Consumidor:** El archivo `spark_streaming_consumidor.py` es una aplicación de Spark Streaming que se conecta al topic de Kafka, recibe los datos en tiempo real y realiza agregaciones sobre ellos en pequeñas ventanas de tiempo.

Este sistema nos permite analizar flujos de datos continuos, una tarea fundamental en aplicaciones de IoT, monitoreo en tiempo real y análisis de eventos.

### 2.2 Implementación y Ejecución

#### Prerrequisitos

- Tener instalado y en ejecución **Apache Kafka** y **Zookeeper** en el mismo entorno que la Actividad anterior.
- Las librerías de Python `kafka-python` y `pyspark`.

#### Pasos de Ejecución

1.  **Iniciar el Productor de Kafka:** Abre una terminal y ejecuta el script del productor. Este comenzará a generar y enviar datos al topic `sensor_data` indefinidamente.
    ```bash
    python3 kafka_sensor_productor.py
    ```

2.  **Iniciar el Consumidor de Spark Streaming:** Abre una segunda terminal y lanza la aplicación de Spark Streaming. Esta se conectará a Kafka y comenzará a procesar los datos que envía el productor.
    ```bash
    # Comando para lanzar una aplicación de Spark Streaming que se conecta a Kafka
    spark_streaming_spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_streaming_consumidor.py
    ```

#### Explicación Detallada de los Códigos

##### `kafka_sensor_productor.py` - Productor:

Este script es el encargado de simular un flujo de datos:
1.  **Generación de Datos:** La función `generar_datos_sensor` crea un diccionario de Python con valores aleatorios para `id_sensor`, `presion`, `nivel_bateria` y `velocidad_viento`, junto con una marca de tiempo.
2.  **Conexión a Kafka:** Se inicializa un `KafkaProducer`, especificando la dirección del servidor de Kafka. El `value_serializer` se encarga de convertir el diccionario de Python a formato JSON y luego a bytes, que es como Kafka transporta los mensajes.
3.  **Bucle de Envío:** Un bucle que es infinito se encarga de:
    - Llamar a `generar_datos_sensor()` para obtener nuevos datos.
    - Enviar los datos al topic `sensor_data`.
    - Imprimir en consola los datos enviados para verificación.
    - Esperar 1 segundo (`time.sleep(1)`) antes de repetir el proceso.

##### `spark_streaming_consumidor.py` - Consumidor

Este script de Spark es el encargado de procesar los datos en tiempo real:
1.  **Inicialización de Spark:** Se crea una `SparkSession` de manera similar a la actividad batch.
2.  **Definición del Esquema:** Se define un `StructType` que describe la estructura y los tipos de datos del JSON que llega desde Kafka, esto es muy importantes para que Spark pueda interpretar correctamente los datos.
3.  **Lectura del Stream:**
    - Se configura una fuente de datos en formato `kafka`.
    - Se especifica el servidor de Kafka y el topic al que se suscribirá.
    - `startingOffsets: el valor "latest"` le indica a Spark que solo procese los mensajes que lleguen a partir del momento en que se inicie la aplicación.
4.  **Parseo y Transformación:**
    - El `value` de Kafka llega como binario, primero se convierte a `string` y luego se utiliza una función para parsear la cadena de texto JSON en una estructura de columnas de Spark.
    - Se convierte el campo `fecha` a un formato de marca de tiempo que Spark puede usar para operaciones de ventana.
5.  **Análisis con Ventanas de Tiempo:**
    - Se agrupan los datos por `id_sensor` y por una ventana de tiempo de 10 segundos.
    - Dentro de cada ventana y para cada sensor, se calculan estadísticas agregadas, las cuales son el promedio, mínimo y máximo de la presión, nivel de batería y velocidad del viento, además de un conteo de los mensajes recibidos.
6.  **Salida de Resultados:**
    - Se configura la salida para que sea la consola.

#### Descripción del Funcionamiento Detallado

El flujo de comunicación es el siguiente:

1.  **El Productor habla:** `kafka_sensor_productor.py` actúa como un dispositivo IoT, cada segundo genera una lectura, la empaqueta en formato JSON y la envía al topic `sensor_data` en el clúster de Kafka.

2.  **Kafka escucha y almacena:** El servidor de Kafka recibe el mensaje del productor y lo almacena de manera duradera y ordenada en una partición del topic `sensor_data`. Kafka actúa como un "buzón" desacoplado, lo que significa que el productor no necesita saber nada sobre el consumidor, y viceversa.

3.  **El Consumidor pregunta:** La aplicación `spark_streaming_consumidor.py` se suscribe al topic `sensor_data`. Periódicamente, Spark consulta a Kafka si hay mensajes nuevos desde la utima consulta que se hizo.

4.  **Kafka responde:** Kafka entrega a Spark todos los mensajes que han llegado desde la última consulta y estos mensajes se cargan en Spark.

5.  **Spark procesa y analiza:** Spark aplica todas las transformaciones como parsear el JSON, extrae el timestamp y actualiza los cálculos de las ventanas de 10 segundos.

Este ciclo se repite continuamente, permitiendo un análisis casi en tiempo real de los datos generados por el sensor.
