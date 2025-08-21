import unittest
from unittest.mock import patch, MagicMock
import os
import sys

# Añadir el directorio del proyecto al path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'docker', 'scrapper')))

from app import configure_selenium, search_product, select_random_product, extract_html, save_html_to_file, upload_to_s3

class TestScrapper(unittest.TestCase):

    @patch('app.ChromeDriverManager')
    @patch('app.webdriver.Chrome')
    def test_configure_selenium(self, mock_chrome, mock_chromedriver_manager):
        print("Probando configure_selenium()...")
        driver = configure_selenium()
        print("Mock de ChromeDriverManager llamado:", mock_chromedriver_manager.called)
        print("Mock de webdriver.Chrome llamado:", mock_chrome.called)
        self.assertTrue(mock_chrome.called)
        self.assertTrue(mock_chromedriver_manager.called)

    @patch('app.WebDriverWait')
    def test_search_product(self, mock_webdriver_wait):
        print("Probando search_product()...")
        mock_driver = MagicMock()
        search_product(mock_driver, "test search")
        print("Método get() de mock_driver fue llamado:", mock_driver.get.called)
        print("WebDriverWait fue llamado:", mock_webdriver_wait.called)
        self.assertTrue(mock_driver.get.called)
        self.assertTrue(mock_webdriver_wait.called)

    @patch('app.WebDriverWait')
    def test_select_random_product(self, mock_webdriver_wait):
        print("Probando select_random_product()...")
        mock_driver = MagicMock()
        mock_webdriver_wait.return_value.until.return_value = [MagicMock()]
        product_url = select_random_product(mock_driver)
        print("URL del producto seleccionado:", product_url)
        self.assertIsNotNone(product_url)

    def test_extract_html(self):
        print("Probando extract_html()...")
        mock_driver = MagicMock()
        mock_driver.page_source = "<html></html>"
        html_content = extract_html(mock_driver)
        print("HTML extraído:", html_content)
        self.assertEqual(html_content, "<html></html>")

    @patch('app.os.makedirs')
    @patch('app.open', new_callable=unittest.mock.mock_open)
    @patch('app.os.path.exists', return_value=True)  # Simula que el archivo existe
    def test_save_html_to_file(self, mock_exists, mock_open, mock_makedirs):
        print("Probando save_html_to_file()...")
        html_content = "<html></html>"
        file_path, file_name = save_html_to_file(html_content)
        print(f"Se intentó guardar el archivo en: {file_path} con nombre: {file_name}")
        self.assertTrue(mock_open.called)
        self.assertTrue(mock_makedirs.called)
        mock_open.assert_called_with(file_path, "w", encoding="utf-8")
        mock_open().write.assert_called_once_with(html_content)
        self.assertTrue(os.path.exists(file_path))  # Se asegura de que el archivo existe

    @patch('app.boto3.client')
    def test_upload_to_s3(self, mock_boto3_client):
        print("Probando upload_to_s3()...")
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        file_path = "test_file.html"
        file_name = "test_file.html"
        s3_key = upload_to_s3(file_path, file_name)
        print(f"Subiendo archivo a S3 con la clave: {s3_key}")
        self.assertTrue(mock_client.upload_file.called)
        self.assertEqual(s3_key, "2023395931/test_file.html")

if __name__ == '__main__':
    unittest.main()
