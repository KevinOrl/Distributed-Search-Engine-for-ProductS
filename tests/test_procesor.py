import unittest
from unittest.mock import patch, MagicMock, call
import json
import os
import sys
import tempfile
from io import StringIO

# Configuramos variables de entorno requeridas para las pruebas
os.environ['RABBITMQ_USER'] = 'test_user'
os.environ['RABBITMQ_PASS'] = 'test_pass'
os.environ['RABBITMQ_QUEUE'] = 'test_queue'
os.environ['RABBITMQ'] = 'test_rabbitmq'
os.environ['MARIADB_USER'] = 'test_user'
os.environ['MARIADB_PASS'] = 'test_pass'
os.environ['MARIADB'] = 'test_mariadb'
os.environ['MARIADB_DB'] = 'test_db'
os.environ['MARIADB_TABLE'] = 'test_table'
os.environ['ELASTICSEARCH_INDEX'] = 'test_index'
os.environ['ELASTICSEARCH_INDEX_DST'] = 'test_index_dst'
os.environ['ELASTICSEARCH_USER'] = 'test_user'
os.environ['ELASTICSEARCH_PASS'] = 'test_pass'
os.environ['ELASTICSEARCH'] = 'test_elasticsearch'
os.environ['BUCKET'] = 'test_bucket'
os.environ['KEY'] = 'test_key'
os.environ['ACCESS_KEY'] = 'test_access_key'
os.environ['SECRET_KEY'] = 'test_secret_key'

# Importamos después de configurar variables de entorno
# Nota: En un entorno real, probablemente importarías el módulo directamente
# pero para la prueba usaremos un mock del archivo

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'docker', 'procesor', 'app')))

class TestDocumentProcessor(unittest.TestCase):
    
    @patch('pika.BlockingConnection')
    @patch('mysql.connector.connect')
    @patch('elasticsearch.Elasticsearch')
    @patch('boto3.client')
    def setUp(self, mock_boto3, mock_es, mock_mysql, mock_pika):
        # Importamos el código del procesador (asumiendo que está en app.py)
        # En una configuración real, importarías el módulo directamente
        from app import DocumentProcessor
        
        # Configuramos los mocks
        self.mock_rabbitmq_connection = mock_pika.return_value
        self.mock_rabbitmq_channel = self.mock_rabbitmq_connection.channel.return_value
        
        self.mock_mariadb_connection = mock_mysql.return_value
        self.mock_mariadb_cursor = self.mock_mariadb_connection.cursor.return_value
        
        self.mock_es = mock_es.return_value
        self.mock_s3 = mock_boto3.return_value
        
        # Creamos la instancia del procesador
        self.processor = DocumentProcessor()
        
    def test_get_document_status(self):
        # Configuramos el mock para simular un documento existente
        self.mock_mariadb_cursor.fetchone.return_value = ['processing']
        
        # Llamamos al método
        status = self.processor.get_document_status('test_doc_id')
        
        # Verificamos que se ejecutó la consulta SQL correcta
        self.mock_mariadb_cursor.execute.assert_called_with(
            "SELECT status FROM test_table WHERE id = %s", 
            ('test_doc_id',)
        )
        
        # Verificamos que se retornó el estado correcto
        self.assertEqual(status, 'processing')
        
        # Configuramos el mock para simular un documento no existente
        self.mock_mariadb_cursor.fetchone.return_value = None
        
        # Llamamos al método
        status = self.processor.get_document_status('nonexistent_doc')
        
        # Verificamos que se retornó None
        self.assertIsNone(status)
    
    def test_update_document_status_existing(self):
        # Configuramos el mock para simular un documento existente
        self.processor.get_document_status = MagicMock(return_value='old_status')
        
        # Llamamos al método
        self.processor.update_document_status('test_doc_id', 'new_status')
        
        # Verificamos que se ejecutó la consulta SQL de actualización
        self.mock_mariadb_cursor.execute.assert_called_with(
            "UPDATE test_table SET status = %s WHERE id = %s", 
            ('test_doc_id', 'new_status')
        )
        
        # Verificamos que se hizo commit a la base de datos
        self.mock_mariadb_connection.commit.assert_called_once()
    
    def test_update_document_status_new(self):
        # Configuramos el mock para simular un documento no existente
        self.processor.get_document_status = MagicMock(return_value=None)
        
        # Llamamos al método
        self.processor.update_document_status('test_doc_id', 'new_status')
        
        # Verificamos que se ejecutó la consulta SQL de inserción
        self.mock_mariadb_cursor.execute.assert_called_with(
            "INSERT INTO test_table (id, status) VALUES (%s, %s)", 
            ('test_doc_id', 'new_status')
        )
        
        # Verificamos que se hizo commit a la base de datos
        self.mock_mariadb_connection.commit.assert_called_once()
    
    def test_get_document_from_elasticsearch(self):
        # Configuramos el mock para simular un documento existente en Elasticsearch
        self.mock_es.get.return_value = {
            "_source": {"content": "<html><body>Test content</body></html>"}
        }
        
        # Llamamos al método
        result = self.processor.get_document_from_elasticsearch('test_doc_id')
        
        # Verificamos que se llamó a Elasticsearch con los parámetros correctos
        self.mock_es.get.assert_called_with(
            index='test_index', 
            id='test_doc_id'
        )
        
        # Verificamos que se retornó el contenido correcto
        self.assertEqual(result, {"content": "<html><body>Test content</body></html>"})
        
        # Configuramos el mock para simular un error
        self.mock_es.get.side_effect = Exception("Not found")
        
        # Llamamos al método
        result = self.processor.get_document_from_elasticsearch('nonexistent_doc')
        
        # Verificamos que se retornó None en caso de error
        self.assertIsNone(result)
    
    def test_save_document_to_elasticsearch(self):
        # Configuramos el mock para simular una operación exitosa
        self.mock_es.index.return_value = {"result": "created"}
        
        # Datos de prueba
        doc_data = {"title": "Test Product", "price": "$10.99"}
        
        # Llamamos al método
        result = self.processor.save_document_to_elasticsearch('test_doc_id', doc_data)
        
        # Verificamos que se llamó a Elasticsearch con los parámetros correctos
        self.mock_es.index.assert_called_with(
            index='test_index_dst',
            id='test_doc_id',
            body=doc_data
        )
        
        # Verificamos que la operación fue exitosa
        self.assertTrue(result)
        
        # Configuramos el mock para simular un error
        self.mock_es.index.side_effect = Exception("Error saving")
        
        # Llamamos al método
        result = self.processor.save_document_to_elasticsearch('test_doc_id', doc_data)
        
        # Verificamos que la operación falló
        self.assertFalse(result)
    
    def test_parse_html_with_beautifulsoup(self):
        # HTML de prueba
        html_content = """
        <html>
            <head>
                <title>Test Product Page</title>
            </head>
            <body>
                <h1 class="product-name">Test Product</h1>
                <span class="price">$10.99</span>
                <div class="description">This is a test product description</div>
                <table class="specifications">
                    <tr>
                        <td>Color</td>
                        <td>Red</td>
                    </tr>
                    <tr>
                        <td>Size</td>
                        <td>Medium</td>
                    </tr>
                </table>
                <img class="product-image" src="/images/product1.jpg" />
                <img class="product-image" src="/images/product2.jpg" />
            </body>
        </html>
        """
        
        # Llamamos al método
        result = self.processor.parse_html_with_beautifulsoup(html_content)
        
        # Verificamos que se extrajo la información correcta
        self.assertEqual(result["title"], "Test Product Page")
        self.assertEqual(result["product_name"], "Test Product")
        self.assertEqual(result["price"], "$10.99")
        self.assertEqual(result["description"], "This is a test product description")
        self.assertEqual(len(result["specifications"]), 2)
        self.assertEqual(result["specifications"][0]["name"], "Color")
        self.assertEqual(result["specifications"][0]["value"], "Red")
        self.assertEqual(len(result["images"]), 2)
        self.assertEqual(result["images"][0], "/images/product1.jpg")
    
    def test_download_file_from_s3(self):
        # Contenido de prueba
        test_content = "<html><body>Test content</body></html>"
        
        # Usamos un archivo temporal para simular la descarga
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file_path = temp_file.name
            temp_file.write(test_content.encode('utf-8'))
        
        # Configuramos el mock para simular la descarga
        def download_file_side_effect(bucket, key, file_path):
            # No hacemos nada aquí ya que ya escribimos en el archivo temporal
            pass
        
        self.mock_s3.download_file.side_effect = download_file_side_effect
        
        # Reemplazamos la función open para usar nuestro archivo temporal
        with patch('builtins.open', create=True) as mock_open:
            mock_open.return_value = open(temp_file_path, 'r')
            
            # Llamamos al método
            result = self.processor.download_file_from_s3('test_doc_id')
        
        # Verificamos que se llamó a S3 con los parámetros correctos
        self.mock_s3.download_file.assert_called()
        
        # Limpiamos el archivo temporal
        os.unlink(temp_file_path)
        
        # Configuramos el mock para simular un error
        self.mock_s3.download_file.side_effect = Exception("Error downloading")
        
        # Llamamos al método
        result = self.processor.download_file_from_s3('test_doc_id')
        
        # Verificamos que la operación falló
        self.assertIsNone(result)
    
    def test_process_message_success(self):
        # Mensaje de prueba
        message_body = json.dumps({"id": "test_doc_id"})
        method = MagicMock()
        method.delivery_tag = "tag1"
        
        # Configuramos mocks para simular un procesamiento exitoso
        self.processor.update_document_status = MagicMock()
        self.processor.get_document_from_elasticsearch = MagicMock(return_value={
            "content": "<html><body>Test content</body></html>"
        })
        self.processor.parse_html_with_beautifulsoup = MagicMock(return_value={
            "title": "Test Product",
            "price": "$10.99"
        })
        self.processor.save_document_to_elasticsearch = MagicMock(return_value=True)
        
        # Llamamos al método
        self.processor.process_message(
            self.mock_rabbitmq_channel, 
            method, 
            None, 
            message_body.encode('utf-8')
        )
        
        # Verificamos la secuencia de llamadas
        self.processor.update_document_status.assert_has_calls([
            call('test_doc_id', 'processing'),
            call('test_doc_id', 'processed')
        ])
        
        self.processor.get_document_from_elasticsearch.assert_called_with('test_doc_id')
        self.processor.parse_html_with_beautifulsoup.assert_called()
        self.processor.save_document_to_elasticsearch.assert_called()
        
        # Verificamos que se confirmó el mensaje
        self.mock_rabbitmq_channel.basic_ack.assert_called_with(delivery_tag="tag1")
    
    def test_process_message_document_not_found(self):
        # Mensaje de prueba
        message_body = json.dumps({"id": "test_doc_id"})
        method = MagicMock()
        method.delivery_tag = "tag1"
        
        # Configuramos mocks para simular un documento no encontrado
        self.processor.update_document_status = MagicMock()
        self.processor.get_document_from_elasticsearch = MagicMock(return_value=None)
        self.processor.download_file_from_s3 = MagicMock(return_value=None)
        
        # Llamamos al método
        self.processor.process_message(
            self.mock_rabbitmq_channel, 
            method, 
            None, 
            message_body.encode('utf-8')
        )
        
        # Verificamos que se actualizó el estado a error
        self.processor.update_document_status.assert_has_calls([
            call('test_doc_id', 'processing'),
            call('test_doc_id', 'error_not_found')
        ])
        
        # Verificamos que se confirmó el mensaje
        self.mock_rabbitmq_channel.basic_ack.assert_called_with(delivery_tag="tag1")
    
    def test_process_message_saving_error(self):
        # Mensaje de prueba
        message_body = json.dumps({"id": "test_doc_id"})
        method = MagicMock()
        method.delivery_tag = "tag1"
        
        # Configuramos mocks para simular un error al guardar
        self.processor.update_document_status = MagicMock()
        self.processor.get_document_from_elasticsearch = MagicMock(return_value={
            "content": "<html><body>Test content</body></html>"
        })
        self.processor.parse_html_with_beautifulsoup = MagicMock(return_value={
            "title": "Test Product",
            "price": "$10.99"
        })
        self.processor.save_document_to_elasticsearch = MagicMock(return_value=False)
        
        # Llamamos al método
        self.processor.process_message(
            self.mock_rabbitmq_channel, 
            method, 
            None, 
            message_body.encode('utf-8')
        )
        
        # Verificamos que se actualizó el estado a error
        self.processor.update_document_status.assert_has_calls([
            call('test_doc_id', 'processing'),
            call('test_doc_id', 'error_saving')
        ])
        
        # Verificamos que se confirmó el mensaje
        self.mock_rabbitmq_channel.basic_ack.assert_called_with(delivery_tag="tag1")
    
    def test_process_message_exception(self):
        # Mensaje de prueba
        message_body = json.dumps({"id": "test_doc_id"})
        method = MagicMock()
        method.delivery_tag = "tag1"
        
        # Configuramos un mock que lanza una excepción
        self.processor.update_document_status = MagicMock(side_effect=Exception("Test error"))
        
        # Llamamos al método
        self.processor.process_message(
            self.mock_rabbitmq_channel, 
            method, 
            None, 
            message_body.encode('utf-8')
        )
        
        # Verificamos que se confirmó el mensaje a pesar del error
        self.mock_rabbitmq_channel.basic_ack.assert_called_with(delivery_tag="tag1")

if __name__ == '__main__':
    unittest.main()