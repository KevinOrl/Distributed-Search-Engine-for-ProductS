import os
import json
import hashlib
import boto3
import pika
import pymysql
from elasticsearch import Elasticsearch

# Variables de entorno
RABBITMQ_USER = os.getenv('RABBITMQ_USER')
RABBITMQ_PASS = os.getenv('RABBITMQ_PASS')
RABBITMQ_QUEUE = os.getenv('RABBITMQ_QUEUE')
RABBITMQ_QUEUE_DST = os.getenv('RABBITMQ_QUEUE_DST')
RABBITMQ = os.getenv('RABBITMQ')

MARIADB_USER = os.getenv('MARIADB_USER')
MARIADB_PASS = os.getenv('MARIADB_PASS')
MARIADB = os.getenv('MARIADB')
MARIADB_DB = os.getenv('MARIADB_DB')
MARIADB_TABLE = os.getenv('MARIADB_TABLE')

ELASTICSEARCH_INDEX = os.getenv('ELASTICSEARCH_INDEX')
ELASTICSEARCH_USER = os.getenv('ELASTICSEARCH_USER')
ELASTICSEARCH_PASS = os.getenv('ELASTICSEARCH_PASS')
ELASTICSEARCH = os.getenv('ELASTICSEARCH')

BUCKET = os.getenv('BUCKET')
KEY = os.getenv('KEY')
ACCESS_KEY = os.getenv('ACCESS_KEY')
SECRET_KEY = os.getenv('SECRET_KEY')

# Configurar cliente de S3
s3_client = boto3.client(
    's3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY
)

# Configurar cliente de Elasticsearch
es = Elasticsearch(
    "http://ic4302-es-http:9200",
    basic_auth=(ELASTICSEARCH_USER, ELASTICSEARCH_PASS)
)

# Configurar conexión a MariaDB
def get_db_connection():
    return pymysql.connect(
        host=MARIADB, user=MARIADB_USER, password=MARIADB_PASS, database=MARIADB_DB
    )

def download_file_from_s3(file_name):
    s3_key = f"{KEY}/{file_name}"
    local_file_path = f"/tmp/{file_name}"
    s3_client.download_file(BUCKET, s3_key, local_file_path)
    print(f"Archivo descargado de S3: {local_file_path}")
    return local_file_path

def read_file_content(local_file_path):
    with open(local_file_path, 'r', encoding='utf-8') as file:
        return file.read()

def store_document_in_elasticsearch(file_content):
    es_response = es.index(index=ELASTICSEARCH_INDEX, document={'content': file_content})
    es_id = es_response['_id']
    print(f"Documento almacenado en Elasticsearch con ID: {es_id}")
    return es_id

def update_mariadb_status(file_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(f"UPDATE {MARIADB_TABLE} SET estado = 'downloaded' WHERE path_documento = %s", (file_name,))
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Estado actualizado en MariaDB para el archivo: {file_name}")

def publish_message_to_rabbitmq(document_id, es_id):
    message = json.dumps({
        'document_id': document_id,
        'elasticsearch_id': es_id
    })
    channel.basic_publish(exchange='', routing_key=RABBITMQ_QUEUE_DST, body=message)
    print(f"Mensaje publicado en RabbitMQ: {message}")

# Función para procesar mensajes de RabbitMQ
def callback(ch, method, properties, body):
    json_object = json.loads(body)
    file_name = json_object['file_name']
    document_id = json_object['document_id']

    # Descargar el archivo de S3
    local_file_path = download_file_from_s3(file_name)
    
    # Leer el contenido del archivo
    file_content = read_file_content(local_file_path)
    
    # Almacenar el documento en Elasticsearch
    es_id = store_document_in_elasticsearch(file_content)
    
    # Actualizar el estado en MariaDB
    update_mariadb_status(file_name)
    
    # Publicar un mensaje en RabbitMQ
    publish_message_to_rabbitmq(document_id, es_id)

# Configurar conexión a RabbitMQ
credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
parameters = pika.ConnectionParameters(host=RABBITMQ, credentials=credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()
channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
channel.basic_consume(queue=RABBITMQ_QUEUE, on_message_callback=callback, auto_ack=True)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()