import unittest
from unittest.mock import patch, MagicMock, mock_open
import os
import sys
import json

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'docker', 'downloader', 'app')))

from app import (
    download_file_from_s3, 
    read_file_content, 
    store_document_in_elasticsearch, 
    update_mariadb_status, 
    publish_message_to_rabbitmq,
    callback,
    get_db_connection
)

class TestMessageProcessor(unittest.TestCase):

    @patch('app.boto3.client')
    def test_download_file_from_s3(self, mock_s3_client):
        print("Probando download_file_from_s3()...")
        mock_client = MagicMock()
        mock_s3_client.return_value = mock_client
        file_name = "test_document.txt"
        expected_local_path = f"/tmp/{file_name}"
        
        result = download_file_from_s3(file_name)
        
        mock_client.download_file.assert_called_once()
        self.assertEqual(result, expected_local_path)
        print(f"Archivo descargado desde S3 to path: {result}")

    @patch('builtins.open', new_callable=mock_open, read_data="test file content")
    def test_read_file_content(self, mock_file):
        print("Probando read_file_content()...")

        local_file_path = "/tmp/test_document.txt"
        expected_content = "test file content"
        
        result = read_file_content(local_file_path)
        
        mock_file.assert_called_once_with(local_file_path, 'r', encoding='utf-8')
        self.assertEqual(result, expected_content)
        print(f"Lectura de archivo: {result}")

    @patch('app.Elasticsearch')
    def test_store_document_in_elasticsearch(self, mock_es):
        print("Probando store_document_in_elasticsearch()...")

        mock_es_instance = MagicMock()
        mock_es.return_value = mock_es_instance
        mock_es_instance.index.return_value = {'_id': 'test_es_id_123'}
        file_content = "test file content"
        
        result = store_document_in_elasticsearch(file_content)
        
        mock_es_instance.index.assert_called_once()
        self.assertEqual(result, 'test_es_id_123')
        print(f"DDocumento almacenado en Elasticsearch con ID: {result}")

    @patch('app.pymysql.connect')
    def test_update_mariadb_status(self, mock_connect):
        print("Probando update_mariadb_status()...")

        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        file_name = "test_document.txt"
        
        update_mariadb_status(file_name)
        
        mock_connect.assert_called_once()
        mock_cursor.execute.assert_called_once()
        mock_connection.commit.assert_called_once()
        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_called_once()
        print(f"Actualización del estado en MariaDB al archivo: {file_name}")

    @patch('app.channel')
    def test_publish_message_to_rabbitmq(self, mock_channel):
        print("Probando publish_message_to_rabbitmq()...")

        document_id = "doc123"
        es_id = "es456"
        expected_message = json.dumps({
            'document_id': document_id,
            'elasticsearch_id': es_id
        })
        
        publish_message_to_rabbitmq(document_id, es_id)
        
        mock_channel.basic_publish.assert_called_once()
        call_args = mock_channel.basic_publish.call_args[1]
        self.assertEqual(call_args['body'], expected_message)
        print(f"Mensaje publicado a RabbitMQ: {expected_message}")

    @patch('app.download_file_from_s3')
    @patch('app.read_file_content')
    @patch('app.store_document_in_elasticsearch')
    @patch('app.update_mariadb_status')
    @patch('app.publish_message_to_rabbitmq')
    def test_callback(self, mock_publish, mock_update, mock_store, mock_read, mock_download):
        print("Probando callback()...")

        ch = MagicMock()
        method = MagicMock()
        properties = MagicMock()
        
        file_name = "test_document.txt"
        document_id = "doc123"
        message_body = json.dumps({
            'file_name': file_name,
            'document_id': document_id
        })
        
        mock_download.return_value = f"/tmp/{file_name}"
        mock_read.return_value = "test file content"
        mock_store.return_value = "es456"
        
        callback(ch, method, properties, message_body)
        
        mock_download.assert_called_once_with(file_name)
        mock_read.assert_called_once()
        mock_store.assert_called_once()
        mock_update.assert_called_once_with(file_name)
        mock_publish.assert_called_once()
        print("Callback al mensaje de RabbitMQ ejecutado correctamente")

    @patch('app.pymysql.connect')
    def test_get_db_connection(self, mock_connect):
        print("Probando get_db_connection()...")

        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        
        result = get_db_connection()
        
        mock_connect.assert_called_once()
        self.assertEqual(result, mock_connection)
        print("Database connection established successfully")

if __name__ == '__main__':
    unittest.main()