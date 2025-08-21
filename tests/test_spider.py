import unittest
from unittest.mock import patch, MagicMock
import os
import sys
import json
import pika
import pymysql
import hashlib
import boto3

# Añadir el directorio del proyecto al path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'docker', 's3-spider', 'app')))

from app import (
    get_db_connection, get_files_in_s3, calculate_md5, get_stored_md5, insert_new_document,
    update_db_status, get_document_id, publish_message, process_file, main
)

class TestScrapper(unittest.TestCase):

    @patch('app.get_db_connection')
    def test_get_db_connection(self, mock_get_db_connection):
        print("[TEST] Probando get_db_connection()...")
        mock_connection = MagicMock()
        mock_get_db_connection.return_value = mock_connection
        conn = get_db_connection()
        print("[TEST] Conexión a la base de datos establecida:", conn)
        self.assertEqual(conn, mock_connection)

    @patch('app.s3_client.list_objects_v2')
    def test_get_files_in_s3(self, mock_list_objects):
        print("[TEST] Probando get_files_in_s3()...")
        mock_list_objects.return_value = {
            'Contents': [{'Key': '2023395931/file1.html'}, {'Key': '2023395931/file2.html'}]
        }
        files = get_files_in_s3()
        print(f"[TEST] Archivos encontrados en S3: {files}")
        self.assertEqual(files, ['file1.html', 'file2.html'])

    @patch('app.s3_client.get_object')
    def test_calculate_md5(self, mock_get_object):
        print("[TEST] Probando calculate_md5()...")
        mock_get_object.return_value = {'Body': MagicMock(read=MagicMock(return_value=b'Hello World'))}
        md5_hash = calculate_md5("file1.html")
        print(f"[TEST] MD5 calculado: {md5_hash}")
        self.assertEqual(md5_hash, hashlib.md5(b'Hello World').hexdigest())

    @patch('app.get_stored_md5')
    def test_get_stored_md5(self, mock_get_stored_md5):
        print("[TEST] Probando get_stored_md5()...")
        mock_get_stored_md5.return_value = '1234567890abcdef'
        stored_md5 = get_stored_md5("file1.html")
        print(f"[TEST] MD5 almacenado en la base de datos: {stored_md5}")
        self.assertEqual(stored_md5, '1234567890abcdef')

    @patch('app.pymysql.connect')
    def test_insert_new_document(self, mock_connect):
        print("[TEST] Probando insert_new_document()...")
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        insert_new_document("file1.html", "1234567890abcdef")
        print(f"[TEST] Inserción de nuevo documento realizada en la base de datos.")
        mock_cursor.execute.assert_called_once()

    @patch('app.pymysql.connect')
    def test_update_db_status(self, mock_connect):
        print("[TEST] Probando update_db_status()...")
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        update_db_status("file1.html", "1234567890abcdef")
        print(f"[TEST] Actualización de documento en la base de datos.")
        mock_cursor.execute.assert_called_once()

    @patch('app.get_document_id')
    def test_get_document_id(self, mock_get_document_id):
        print("[TEST] Probando get_document_id()...")
        mock_get_document_id.return_value = 1
        document_id = get_document_id("file1.html")
        print(f"[TEST] ID del documento en la base de datos: {document_id}")
        self.assertEqual(document_id, 1)

    @patch('app.pika.BlockingConnection')
    def test_publish_message(self, mock_blocking_connection):
        print("[TEST] Probando publish_message()...")
        mock_connection = MagicMock()
        mock_channel = MagicMock()
        mock_connection.channel.return_value = mock_channel
        mock_blocking_connection.return_value = mock_connection
        publish_message("file1.html", "new", 1)
        print("[TEST] Mensaje publicado en RabbitMQ.")
        mock_channel.basic_publish.assert_called_once()

    @patch('app.calculate_md5')
    @patch('app.get_stored_md5')
    @patch('app.insert_new_document')
    @patch('app.update_db_status')
    @patch('app.publish_message')
    def test_process_file(self, mock_publish_message, mock_update_db_status, mock_insert_new_document, mock_get_stored_md5, mock_calculate_md5):
        print("[TEST] Probando process_file()...")

        # Caso: archivo nuevo
        mock_calculate_md5.return_value = "new_md5"
        mock_get_stored_md5.return_value = None
        process_file("file1.html")
        print("[TEST] Proceso de archivo nuevo completado.")
        mock_insert_new_document.assert_called_once()

        # Caso: archivo actualizado
        mock_get_stored_md5.return_value = "old_md5"
        process_file("file2.html")
        print("[TEST] Proceso de archivo actualizado completado.")
        mock_update_db_status.assert_called_once()

        # Caso: archivo sin cambios
        mock_get_stored_md5.return_value = "new_md5"
        process_file("file3.html")
        print("[TEST] Proceso de archivo sin cambios completado.")

    def test_main(self):
        with patch('app.get_files_in_s3', return_value=['file1.html', 'file2.html']), \
             patch('app.process_file') as mock_process_file:
            print("[TEST] Probando main()...")
            main()
            print("[TEST] Verificando que process_file fue llamado...")
            mock_process_file.assert_called_with('file2.html')

if __name__ == '__main__':
    unittest.main()
