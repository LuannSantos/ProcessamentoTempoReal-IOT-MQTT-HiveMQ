import random
import time
import pandas as pd
import numpy as np
import pendulum
import os
import json
import boto3
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
CLIENT_ID = f'{CLIENT_ID}-{random.randint(1000, 2000)}'

AWSACCESSKEYID = os.environ.get("AWSACCESSKEYID")
AWSSECRETKEY = os.environ.get("AWSSECRETKEY")
BUCKET_NAME = os.environ.get("BUCKET_NAME")

# Objeto para conexão ao S3
s3 = boto3.resource(
    's3',
    aws_access_key_id=AWSACCESSKEYID,
    aws_secret_access_key=AWSSECRETKEY
)

# Nome do arquivo de log
nome_arquivo_log = 'mqtt_subscriber'

# Tabela pandas que armazena os dados
df = pd.DataFrame({'time_id': pd.Series(dtype='str'),
                   'ping_ms': pd.Series(dtype='float'),
                   'temperature_c': pd.Series(dtype='int'),
                   'humidity_p': pd.Series(dtype='int')})

# Realiza a conexão ao broker
def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            registrar_log(f'Conectado ao BROKER MQTT subscribe, no topico {TOPIC}',nome_arquivo_log)
        else:
            registrar_log(f'Falha de conexão ao subscribe, código do erro: {rc}',nome_arquivo_log)

    client = mqtt_client.Client(CLIENT_ID)
    client.username_pw_set(BROKER_USERNAME, BROKER_PASSWORD)
    client.on_connect = on_connect
    client.connect(BROKER_HOST, BROKER_PORT)
    return client


# Recebe e processa as mensagens
def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        global df

        registrar_log(f"Recebido `{msg.payload.decode()}` do topico `{msg.topic}`",nome_arquivo_log)
        msg_str = msg.payload.decode().replace("'", '"')
        dados = json.loads(msg_str) 

        df_temp = pd.DataFrame(data = dados,index=[0])
        df = pd.concat( [df,df_temp] )
        df['datetime_time_id'] = pd.to_datetime(df['time_id'])

        df['moving_mean_temperature'] = df.rolling('1h', on = 'datetime_time_id')['temperature_c'].mean()
        df['moving_mean_humidity'] = df.rolling('1h', on = 'datetime_time_id')['humidity_p'].mean()

        path = os.getcwd()
        path = path.replace("\\","/")
        agora = pendulum.now()
        data_arquivo = agora.format('YYYYMMDDHH')
        nome_arquivo =  data_arquivo + "_data.csv"
        caminho_arquivo = path + "/output/" + nome_arquivo

        df_csv = df[['datetime_time_id', 'ping_ms', 'temperature_c', 'humidity_p', 'moving_mean_temperature', 'moving_mean_humidity']]

        df_csv.to_csv(path_or_buf = caminho_arquivo, index=False)
        s3.Bucket(BUCKET_NAME).upload_file(caminho_arquivo, 'mqtt/output/' + nome_arquivo)

    client.subscribe(TOPIC)
    client.on_message = on_message


def run():
    client = connect_mqtt()
    subscribe(client)
    client.loop_forever()


if __name__ == '__main__':
    run()