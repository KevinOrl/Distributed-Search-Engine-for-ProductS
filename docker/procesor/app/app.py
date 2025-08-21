import os
import json
import time
import logging
import boto3
from bs4 import BeautifulSoup
import pika
import pymysql
from elasticsearch import Elasticsearch
import requests
from requests.auth import HTTPBasicAuth
import tempfile

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('document_processor')

# Obtener variables de entorno
RABBITMQ_USER = os.getenv('RABBITMQ_USER')
RABBITMQ_PASS = os.getenv('RABBITMQ_PASS')
RABBITMQ_QUEUE = os.getenv('RABBITMQ_QUEUE')
RABBITMQ = os.getenv('RABBITMQ')

MARIADB_USER = os.getenv('MARIADB_USER')
MARIADB_PASS = os.getenv('MARIADB_PASS')
MARIADB = os.getenv('MARIADB')
MARIADB_DB = os.getenv('MARIADB_DB')
MARIADB_TABLE = os.getenv('MARIADB_TABLE')

ELASTICSEARCH_INDEX = os.getenv('ELASTICSEARCH_INDEX')
ELASTICSEARCH_INDEX_DST = os.getenv('ELASTICSEARCH_INDEX_DST')
ELASTICSEARCH_USER = os.getenv('ELASTICSEARCH_USER')
ELASTICSEARCH_PASS = os.getenv('ELASTICSEARCH_PASS')
ELASTICSEARCH = os.getenv('ELASTICSEARCH')

BUCKET = os.getenv('BUCKET')
KEY = os.getenv('KEY')
ACCESS_KEY = os.getenv('ACCESS_KEY')
SECRET_KEY = os.getenv('SECRET_KEY')



class DocumentProcessor:
    def __init__(self):
        self.connect_rabbitmq()
        self.connect_mariadb()
        self.connect_elasticsearch()
        self.connect_s3()

    def connect_rabbitmq(self):
        logger.info(f"Conectando a RabbitMQ en {RABBITMQ}")
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
        parameters = pika.ConnectionParameters(
            host=RABBITMQ,
            credentials=credentials,
            connection_attempts=5,
            retry_delay=5
        )
        self.rabbitmq_connection = pika.BlockingConnection(parameters)
        self.rabbitmq_channel = self.rabbitmq_connection.channel()
        self.rabbitmq_channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
        logger.info("Conexión a RabbitMQ establecida")

    def connect_mariadb(self):
        logger.info(f"Conectando a MariaDB en {MARIADB}")
        self.mariadb_connection = pymysql.connect(
            host=MARIADB,
            user=MARIADB_USER,
            password=MARIADB_PASS,
            database=MARIADB_DB,
            # No necesitas especificar collation con pymysql
        )
        self.mariadb_cursor = self.mariadb_connection.cursor()
        
        # El resto del código puede permanecer igual
        self.mariadb_cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {MARIADB_TABLE} (
                id VARCHAR(255) PRIMARY KEY,
                status VARCHAR(50),
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            )
        """)
        self.mariadb_connection.commit()
        logger.info("Conexión a MariaDB establecida")

    def connect_elasticsearch(self):
        logger.info(f"Conectando a Elasticsearch en {ELASTICSEARCH}")
        self.es = Elasticsearch(
            [ELASTICSEARCH],
            http_auth=(ELASTICSEARCH_USER, ELASTICSEARCH_PASS),
            verify_certs=False
        )
        logger.info("Conexión a Elasticsearch establecida")

    def connect_s3(self):
        logger.info("Configurando cliente S3")
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY
        )
        logger.info("Cliente S3 configurado")

    def get_document_status(self, doc_id):
        """Obtiene el estado del documento en MariaDB"""
        query = f"SELECT status FROM {MARIADB_TABLE} WHERE id = %s"
        self.mariadb_cursor.execute(query, (doc_id,))
        result = self.mariadb_cursor.fetchone()
        if result:
            return result[0]
        return None

    def update_document_status(self, doc_id, status):
        """Actualiza el estado de un documento en MariaDB"""
        # Siempre usar "processed" como estado
        status = "processed"
        
        try:
            # La tabla real se llama 'objects' y debemos buscar por o id numérico
            # Primero intentamos actualizar por id
            self.mariadb_cursor.execute(f"UPDATE {MARIADB_TABLE} SET estado = %s WHERE id = %s", (status, int(doc_id)))
            
            self.mariadb_connection.commit()
            logger.info(f"Estado del documento {doc_id} actualizado a: {status}")
        except Exception as e:
            logger.error(f"Error al actualizar estado del documento {doc_id}: {e}")
            # Intenta reconectar si la conexión se perdió
            self.connect_mariadb()

    def get_document_from_elasticsearch(self, doc_elasticsearch_id):
        """Obtiene el documento desde Elasticsearch"""
        try:
            # Corregido: usar id en lugar de _id
            result = self.es.get(index=ELASTICSEARCH_INDEX, id=doc_elasticsearch_id)
            return result["_source"]
        except Exception as e:
            logger.error(f"Error al obtener documento de Elasticsearch: {e}")
            return None

    def save_document_to_elasticsearch(self, doc_id, document_data):
        """Guarda el documento procesado en Elasticsearch"""
        try:
            self.es.index(
                index=ELASTICSEARCH_INDEX_DST,
                id=doc_id,
                body=document_data
            )
            logger.info(f"Documento {doc_id} guardado en {ELASTICSEARCH_INDEX_DST}")
            return True
        except Exception as e:
            logger.error(f"Error al guardar documento en Elasticsearch: {e}")
            return False

    def parse_html_with_beautifulsoup(self, html_content):
        """Parsea el contenido HTML con BeautifulSoup para extraer información básica del producto de eBay"""
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Inicializar el diccionario de información del producto 
        product_info = {
            "title": soup.title.string if soup.title else "Sin título",
            "product_name": "",
            "price": "",
            "description": "",
            "categories": [],
            "images": []
        }
        
        try:
            # 1. NOMBRE DEL PRODUCTO 
            product_name_candidates = [
                soup.find('h1', {'class': 'x-item-title__mainTitle'}),
                soup.find('h1', {'id': 'itemTitle'}),
                soup.find('h1', {'class': 'product-title'})
            ]
            
            for candidate in product_name_candidates:
                if candidate:
                    name_text = candidate.get_text(strip=True)
                    # Limpiar prefijos comunes de eBay como "Details about"
                    if name_text.startswith("Details about"):
                        name_text = name_text.replace("Details about", "").strip()
                    product_info["product_name"] = name_text
                    break
                    
            # Si no se encontró el nombre, usar el título
            if not product_info["product_name"] and product_info["title"]:
                clean_title = product_info["title"].replace(" | eBay", "").strip()
                product_info["product_name"] = clean_title
            
            # 2. PRECIO - Buscar el precio principal
            price_candidates = [
                soup.find(class_=lambda x: x and 'x-price-primary' in x),
                soup.find(class_=lambda x: x and 'displayPrice' in x),
                soup.find('span', {'id': 'prcIsum'}),
                soup.find('span', {'itemprop': 'price'})
            ]
            
            for candidate in price_candidates:
                if candidate:
                    product_info["price"] = candidate.get_text(strip=True)
                    break
            
            description_found = False
            
            # Primero buscar si hay un iframe de descripción (común en eBay)
            iframe = soup.find('iframe', {'id': 'desc_ifr'})
            if iframe and 'src' in iframe.attrs:
                iframe_url = iframe['src']
                logger.info(f"Encontrado iframe de descripción con URL: {iframe_url}")
                
                try:
                    iframe_response = requests.get(iframe_url, timeout=10)
                    if iframe_response.status_code == 200:
                        iframe_soup = BeautifulSoup(iframe_response.text, 'html.parser')
                        
                        if iframe_soup.body:
                            product_info["description"] = iframe_soup.body.get_text(strip=True)
                            description_found = True
                            logger.info("Descripción extraída del iframe correctamente")
                except Exception as iframe_error:
                    logger.error(f"Error al obtener contenido del iframe: {iframe_error}")
            
            # Si no se encontró descripción en el iframe, buscar en el HTML principal
            if not description_found:
                desc_elem = soup.find(id='desc_div') or soup.find(class_=lambda x: x and 'description' in x)
                if desc_elem:
                    product_info["description"] = desc_elem.get_text(strip=True)
                    description_found = True
                else:
                    desc_container = soup.find('div', {'data-marko-key': '@container s0-14', 'class': 'x-item-description-child'})
                    if desc_container:
                        product_info["description"] = desc_container.get_text(strip=True)
                        description_found = True
                    else:
                        item_desc = soup.find('div', {'class': 'item-desc'}) or soup.find('div', {'class': 'prodDetailSec'})
                        if item_desc:
                            product_info["description"] = item_desc.get_text(strip=True)
                            description_found = True
            
            # 4. CATEGORÍAS - Extraer categorías 
            seo_breadcrumbs = soup.find_all('a', {'class': 'seo-breadcrumb-text'})
            if seo_breadcrumbs:
                for breadcrumb in seo_breadcrumbs:
                    
                    span = breadcrumb.find('span')
                    if span:
                        cat_text = span.get_text(strip=True)
                    else:
                        cat_text = breadcrumb.get_text(strip=True)
                        
                    if cat_text and cat_text not in product_info["categories"]:
                        product_info["categories"].append(cat_text)
                logger.info(f"Categorías extraídas de seo-breadcrumb-text: {product_info['categories']}")
            
            # Si no se encontraron categorías con seo-breadcrumb-text, buscar en breadcrumbs regulares
            if not product_info["categories"]:
                breadcrumb = soup.find('nav', {'class': 'breadcrumb'}) or soup.find('ul', {'class': 'breadcrumbs'})
                if breadcrumb:
                    category_elements = breadcrumb.find_all('a') or breadcrumb.find_all('li')
                    for cat in category_elements:
                        cat_text = cat.get_text(strip=True)
                        if cat_text and cat_text not in ["Home", "Back to home page"]:
                            product_info["categories"].append(cat_text)
            
            # 5. IMÁGENES - Extraer imágenes del producto
            image_containers = [
                soup.find('div', {'id': 'mainImgHldr'}),
                soup.find('div', {'class': 'ux-image-carousel'})
            ]
            
            for container in image_containers:
                if container:
                    images = container.find_all('img')
                    for img in images:
                        for attr in ['src', 'data-src', 'data-img-src', 'data-zoom-src']:
                            if attr in img.attrs:
                                img_src = img[attr]
                                # Convertir URLs relativas a absolutas
                                if img_src.startswith('//'):
                                    img_src = 'https:' + img_src
                                # Ignorar imágenes muy pequeñas o iconos
                                if 'gif' not in img_src.lower() and 'icon' not in img_src.lower():
                                    product_info["images"].append(img_src)
                                    break
            
            # Si no se encontraron imágenes, buscar alternativas
            if not product_info["images"]:
                ebay_images = soup.find_all('img', src=lambda src: src and 'i.ebayimg.com' in src)
                for img in ebay_images:
                    img_src = img['src']
                    if 's-l64' not in img_src and 's-l32' not in img_src:
                        if img_src.startswith('//'):
                            img_src = 'https:' + img_src
                        product_info["images"].append(img_src)
            
            # Mejorar URLs de imágenes para obtener tamaños más grandes
            for i, img_url in enumerate(product_info["images"]):
                # Reemplazar versiones pequeñas con versiones grandes cuando sea posible
                for size in ['140', '225', '300']:
                    if f's-l{size}' in img_url:
                        product_info["images"][i] = img_url.replace(f's-l{size}', 's-l500')
            
            # Deduplicar imágenes
            product_info["images"] = list(dict.fromkeys(product_info["images"]))
            
        except Exception as e:
            logger.error(f"Error al parsear HTML: {str(e)}")
        
        # Depuración para ver qué se extrajo
        logger.info(f"Información extraída: Nombre={product_info['product_name'][:30]}..., " +
                    f"Precio={product_info['price']}, Categorías={product_info['categories']}, " +
                    f"Imágenes={len(product_info['images'])}")
        
        return product_info

    def download_file_from_s3(self, doc_id):
        """Descarga un archivo de S3"""
        try:
            file_path = os.path.join(KEY, f"{doc_id}.html")
            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                self.s3_client.download_file(BUCKET, file_path, temp_file.name)
                temp_file_path = temp_file.name
            
            with open(temp_file_path, 'r', encoding='utf-8') as file:
                content = file.read()
            
            os.unlink(temp_file_path)
            return content
        except Exception as e:
            logger.error(f"Error al descargar archivo de S3: {e}")
            return None

    def process_message(self, ch, method, properties, body):
        """Procesa un mensaje de RabbitMQ"""
        try:
            message = json.loads(body)
            logger.info(f"Contenido completo del mensaje recibido: {message}")
            
            # Obtener el ID del documento y convertirlo a string
            doc_id = message.get('document_id')
            elasticsearch_id = message.get('elasticsearch_id')
            
            if doc_id is not None:
                doc_id = str(doc_id)  # Convertir explícitamente a string
            
            if not doc_id or not elasticsearch_id:
                logger.error(f"Mensaje sin IDs requeridos. document_id: {doc_id}, elasticsearch_id: {elasticsearch_id}")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            
            logger.info(f"Procesando documento {doc_id} con elasticsearch_id {elasticsearch_id}")
            
            self.update_document_status(doc_id, "processed")
            
            # Obtener documento de Elasticsearch usando el elasticsearch_id
            es_doc = self.get_document_from_elasticsearch(elasticsearch_id)
            
            if not es_doc:
                logger.warning(f"Documento con elasticsearch_id {elasticsearch_id} no encontrado en Elasticsearch")
                # Intentar obtener desde S3
                html_content = self.download_file_from_s3(doc_id)
                if not html_content:
                    self.update_document_status(doc_id, "error_not_found")
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    return
            else:
                html_content = es_doc.get('content', '')
            
            # Parsear el contenido HTML
            product_info = self.parse_html_with_beautifulsoup(html_content)
            
            # Guardar información extraída en Elasticsearch
            if self.save_document_to_elasticsearch(doc_id, product_info):
                self.update_document_status(doc_id, "processed")
            else:
                self.update_document_status(doc_id, "error_saving")
            
            # Confirmar mensaje procesado
            ch.basic_ack(delivery_tag=method.delivery_tag)
            logger.info(f"Documento {doc_id} procesado correctamente")
            
        except Exception as e:
            logger.error(f"Error al procesar mensaje: {e}")
            # Confirmar el mensaje para no reprocesarlo continuamente
            ch.basic_ack(delivery_tag=method.delivery_tag)
            # Si se pudo identificar el documento, actualizar su estado
            if locals().get('doc_id'):
                self.update_document_status(doc_id, "error_processing")

    def start_consuming(self):
        """Inicia el consumo de mensajes de RabbitMQ"""
        logger.info(f"Iniciando consumo de mensajes de la cola {RABBITMQ_QUEUE}")
        self.rabbitmq_channel.basic_qos(prefetch_count=1)
        self.rabbitmq_channel.basic_consume(
            queue=RABBITMQ_QUEUE,
            on_message_callback=self.process_message
        )
        try:
            self.rabbitmq_channel.start_consuming()
        except KeyboardInterrupt:
            self.rabbitmq_channel.stop_consuming()
        except Exception as e:
            logger.error(f"Error en consumo de mensajes: {e}")
            self.rabbitmq_channel.stop_consuming()
        
        self.cleanup()

    def cleanup(self):
        """Cierra las conexiones"""
        logger.info("Cerrando conexiones...")
        if hasattr(self, 'rabbitmq_connection') and self.rabbitmq_connection.is_open:
            self.rabbitmq_connection.close()
        
        if hasattr(self, 'mariadb_connection'):
            # pymysql no tiene is_connected(), verificamos de otra manera
            try:
                self.mariadb_cursor.close()
                self.mariadb_connection.close()
            except:
                pass
        
        logger.info("Conexiones cerradas")

# Función principal
def main():
    try:
        processor = DocumentProcessor()
        processor.start_consuming()
    except Exception as e:
        logger.critical(f"Error fatal en la aplicación: {e}")
        exit(1)

if __name__ == "__main__":
    # Esperar un poco para asegurar que los servicios estén disponibles
    time.sleep(5)
    main()