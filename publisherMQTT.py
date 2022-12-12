import random
import time
import pandas as pd
import numpy as np
import pendulum
import os
from dotenv import load_dotenv, find_dotenv
from registrar_log.registrar_log import registrar_log
from paho.mqtt import client as mqtt_client

load_dotenv(find_dotenv())

# Parâmetros MQTT
BROKER_HOST = os.environ.get("BROKER_HOST")
BROKER_PORT = int(os.environ.get("BROKER_PORT"))
TOPIC = os.environ.get("TOPIC")
CLIENT_ID = os.environ.get("CLIENT_ID")
BROKER_PASSWORD = os.environ.get("BROKER_PASSWORD")
BROKER_USERNAME = os.environ.get("BROKER_USERNAME")

# Gera client_id aleatório
CLIENT_ID = f'{CLIENT_ID}-{random.randint(0, 1000)}'

# Nome do arquivo de log
nome_arquivo_log = 'mqtt_publish'

# Realiza a conexão ao broker
def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            registrar_log("Conectado ao BROKER MQTT",nome_arquivo_log)
        else:
            registrar_log(f'Falha de conexão, código do erro: {rc}',nome_arquivo_log)

    client = mqtt_client.Client(CLIENT_ID)
    client.username_pw_set(BROKER_USERNAME, BROKER_PASSWORD)
    client.on_connect = on_connect
    client.connect(BROKER_HOST, BROKER_PORT)
    return client

# Recebe e processa as mensagens
def publish(client):

    df = pd.read_csv("./data/data.csv",dtype={'time_id':'str','ping_ms':'float64','temperature_c':'int64','humidity_p':'int64'})

    for index, row in df.iterrows():
        if index > 0 and index < (df.shape[0] - 1):
            time_id_datetime_0 = pendulum.parse(df['time_id'][index])
            time_id_datetime_1 = pendulum.parse(df['time_id'][index+1])
            delta = time_id_datetime_1 - time_id_datetime_0
            time.sleep(delta.in_seconds())
            #time.sleep(5)

        msg = {'time_id': row['time_id'],
         'temperature_c': row['temperature_c'], 'humidity_p': row['humidity_p'], 'ping_ms': row['ping_ms']}
        msg = str(msg)
        result = client.publish(TOPIC, msg)
        status = result[0]

        if status == 0:
            registrar_log(f"Mensagem `{msg}` enviada ao tópico `{TOPIC}`",nome_arquivo_log)
        else:
            registrar_log(f"Falha ao enviar mensagem ao tópico {TOPIC}",nome_arquivo_log)


def run():
    client = connect_mqtt()
    client.loop_start()
    publish(client)


if __name__ == '__main__':
    run()