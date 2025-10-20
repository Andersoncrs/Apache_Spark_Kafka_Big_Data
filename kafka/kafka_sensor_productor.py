import time
import json
import random
from kafka import KafkaProducer

def generar_datos_sensor():
    return {
        "id_sensor": random.randint(1, 10),
        "presion": round(random.uniform(950, 1050), 2),     # en hPa
        "nivel_bateria": random.randint(0, 100),            # en %
        "velocidad_viento": round(random.uniform(0, 50), 2),# en km/h
        "fecha": int(time.time())
    }

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode('utf-8')
)

while True:
    datos_sensor = generar_datos_sensor()
    producer.send('sensor_data', value=datos_sensor)
    print(f"Enviado: {datos_sensor}")
    time.sleep(1)
