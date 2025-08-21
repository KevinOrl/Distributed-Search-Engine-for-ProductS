import os
import sys
import time
import json
import pika
import hashlib
import boto3
import pymysql

# Variables de entorno
BUCKET = os.getenv('BUCKET')
KEY = os.getenv('KEY', '2023395931')  # Carpeta dentro del bucket
ACCESS_KEY = os.getenv('ACCESS_KEY')
SECRET_KEY = os.getenv('SECRET_KEY')

RABBITMQ_USER = os.getenv('RABBITMQ_USER')
RABBITMQ_PASS = os.getenv('RABBITMQ_PASS')
RABBITMQ_QUEUE = os.getenv('RABBITMQ_QUEUE')
RABBITMQ = os.getenv('RABBITMQ')

MARIADB_USER = os.getenv('MARIADB_USER')
MARIADB_PASS = os.getenv('MARIADB_PASS')
MARIADB = os.getenv('MARIADB')
MARIADB_DB = os.getenv('MARIADB_DB')
MARIADB_TABLE = os.getenv('MARIADB_TABLE')

# Cliente de S3 con credenciales proporcionadas
s3_client = boto3.client(
    's3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY
)
#establece la conexion con la base de datos
def get_db_connection():
    """Establece una conexión a la base de datos MariaDB."""
    return pymysql.connect(
        host=MARIADB, user=MARIADB_USER, password=MARIADB_PASS, database=MARIADB_DB
    )
#Esta funcion trae los files del bucket
def get_files_in_s3():
    """Obtiene la lista de archivos en el bucket de S3."""
    try:
        response = s3_client.list_objects_v2(Bucket=BUCKET, Prefix=KEY)
        return [item['Key'].replace(KEY + '/', '') for item in response.get('Contents', []) if item['Key'] != KEY]
    except Exception as e:
        print(f"[S3] Error al listar archivos: {e}")
        return []
#Esta funcion Calcula el hash MD5 de un archivo en S3.
def calculate_md5(file_name):
    """Calcula el hash MD5 de un archivo en S3."""
    try:
        obj = s3_client.get_object(Bucket=BUCKET, Key=f"{KEY}/{file_name}")
        return hashlib.md5(obj['Body'].read()).hexdigest()
    except Exception as e:
        print(f"[S3] Error al calcular MD5 de {file_name}: {e}")
        return None

#Esta funcion trae el hash md5 del documeto
def get_stored_md5(file_name):
    """Obtiene el hash MD5 almacenado en la base de datos."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT md5_hash FROM {MARIADB_TABLE} WHERE path_documento = %s", (file_name,))
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        return result[0] if result else None
    except Exception as e:
        print(f"[DB] Error al obtener MD5 de {file_name}: {e}")
        return None
#Esta funcion inserta un documento en la base de datos
def insert_new_document(file_name, md5_hash):
    """Inserta un nuevo documento en la base de datos."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        query = f"INSERT INTO {MARIADB_TABLE} (path_documento, estado, md5_hash) VALUES (%s, %s, %s)"
        cursor.execute(query, (file_name, "new", md5_hash))
        conn.commit()
        cursor.close()
        conn.close()
        print(f"[DB] Nuevo documento registrado: {file_name}")
    except Exception as e:
        print(f"[DB] Error al insertar documento: {e}")

#Funcion que actualiza el estado de un documento
def update_db_status(file_name, md5_hash):
    """Actualiza el estado y hash de un documento en la base de datos."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        query = f"UPDATE {MARIADB_TABLE} SET md5_hash = %s, estado = 'updated' WHERE path_documento = %s"
        cursor.execute(query, (md5_hash, file_name))
        conn.commit()
        cursor.close()
        conn.close()
        print(f"[DB] Documento actualizado: {file_name}")
    except Exception as e:
        print(f"[DB] Error al actualizar documento: {e}")

#Esta funcion toma el Id de un documento en la base de datos
def get_document_id(file_name):
    """Obtiene el ID del documento almacenado en la base de datos."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT id FROM {MARIADB_TABLE} WHERE path_documento = %s", (file_name,))
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        return result[0] if result else None
    except Exception as e:
        print(f"[DB] Error al obtener el ID de {file_name}: {e}")
        return None

#esta funcion publica el mensaje en rabbitmq con las propiedades del documento
def publish_message(file_name, status, document_id):
    """Publica un mensaje en RabbitMQ indicando el estado del archivo y el ID."""
    try:
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
        parameters = pika.ConnectionParameters(host=RABBITMQ, credentials=credentials)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)

        message = json.dumps({
            "file_name": file_name,
            "status": status,
            "path": f"{KEY}/{file_name}",
            "document_id": document_id
        })
        channel.basic_publish(exchange='', routing_key=RABBITMQ_QUEUE, body=message)
        print(f"[RabbitMQ] Mensaje publicado: {file_name} - {status} - ID: {document_id}")

        connection.close()
    except Exception as e:
        print(f"[RabbitMQ] Error al publicar mensaje: {e}")

#Esta funcion procesa un documento que se le envia por parametro y pasa por el proceso de extraccion del md5
def process_file(file_name):
    """Procesa un archivo en S3 verificando si es nuevo o ha sido actualizado."""
    print(f"[INFO] Procesando archivo: {file_name}")

    if not file_name or file_name.strip() == '':
        print(f"[ERROR] El archivo {file_name} tiene un nombre vacío, omitiendo...")
        return

    md5_hash = calculate_md5(file_name)
    if not md5_hash:
        print(f"[ERROR] No se pudo calcular MD5 de {file_name}, omitiendo...")
        return

    stored_md5 = get_stored_md5(file_name)

    if stored_md5 is None:
        insert_new_document(file_name, md5_hash)
        document_id = get_document_id(file_name)
        publish_message(file_name, "new", document_id)
    elif stored_md5 != md5_hash:
        update_db_status(file_name, md5_hash)
        document_id = get_document_id(file_name)
        publish_message(file_name, "updated", document_id)
    else:
        print(f"[INFO] No hay cambios en {file_name}")

#Funcion principal que inicia el proceso
def main():
    print("[INFO] Iniciando proceso de monitoreo de archivos en S3...")
    files_in_s3 = get_files_in_s3()

    if not files_in_s3:
        print("[INFO] No hay archivos en el bucket, finalizando ejecución.")
        sys.exit(0)

    for file in files_in_s3:
        process_file(file)

    print("[INFO] Proceso completado. Esperando próximo ciclo...")
    print("--------------------------------------------------------")
    
main()
