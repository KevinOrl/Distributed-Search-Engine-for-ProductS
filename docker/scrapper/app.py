import time
import os
import boto3
import random
from webdriver_manager.chrome import ChromeDriverManager
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime
import sys

# Configura credenciales de AWS
AWS_ACCESS_KEY = "############"
AWS_SECRET_KEY = "############"
BUCKET_NAME = "############"

SEARCH_TERMS = [
    "headphones", "phone case", "smart watch", "laptop stand", "gaming mouse",
    "bluetooth speaker", "usb c cable", "wireless charger", "hdmi cable",
    "kitchen gadget", "home decor", "fitness tracker", "camera accessories",
    "portable charger", "earbuds", "CPU"
]

def configure_selenium():
    """Configura Selenium para Chrome."""
    chrome_options = Options()
    chrome_options.add_argument("--window-size=1920x1080")
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver

def search_product(driver, search_term):
    """Busca un producto en eBay."""
    driver.get("https://www.ebay.com")
    time.sleep(3)
    try:
        search_box = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='search']"))
        )
        search_box.clear()
        search_box.send_keys(search_term)
        search_box.send_keys(Keys.RETURN)
        time.sleep(5)
    except Exception as e:
        print(f"Error al buscar: {e}")
        driver.get(f"https://www.ebay.com/sch/i.html?_nkw={search_term}")
        time.sleep(5)

def select_random_product(driver):
    """Selecciona un producto aleatorio de la lista de búsqueda."""
    try:
        product_links = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "li.s-item a.s-item__link"))
        )
        if not product_links:
            raise Exception("No se encontraron productos")
        random_product = random.choice(product_links[:10])
        product_url = random_product.get_attribute('href')
        
        # Modificar el atributo target para que se abra en la misma ventana
        driver.execute_script("arguments[0].setAttribute('target', '_self');", random_product)
        
        random_product.click()
        time.sleep(5)
        return product_url
    except Exception as e:
        print(f"Error al seleccionar producto: {e}")
        return None

def extract_html(driver):
    """Extrae el HTML de la página actual."""
    return driver.page_source

def save_html_to_file(html_content, folder_name="PO/local_files"):
    """Guarda el HTML en un archivo local."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"ebay_product_{timestamp}.html"
    os.makedirs(folder_name, exist_ok=True)
    file_path = os.path.join(folder_name, file_name)
    with open(file_path, "w", encoding="utf-8") as file:
        file.write(html_content)
    return file_path, file_name

def upload_to_s3(file_path, file_name):
    """Sube el archivo a S3."""
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name="us-east-1"
    )
    s3_key = f"2023395931/{file_name}"
    s3_client.upload_file(file_path, BUCKET_NAME, s3_key)
    return s3_key

def main():
    """Ejecuta el flujo completo: búsqueda, extracción y carga en S3."""
    while True:
        driver = configure_selenium()
        file_path = None
        try:
            random_search = random.choice(SEARCH_TERMS)
            print(f"Búsqueda seleccionada: {random_search}")
            search_product(driver, random_search)
            product_url = select_random_product(driver)
            if product_url:
                print(f"Producto seleccionado: {product_url}")
            html_content = extract_html(driver)
            file_path, file_name = save_html_to_file(html_content)
            print(f"Archivo guardado: {file_name}")
            s3_key = upload_to_s3(file_path, file_name)
            print(f"Subido a S3: s3://{BUCKET_NAME}/{s3_key}")
        except Exception as e:
            print(f"Error: {e}")
        finally:
            driver.quit()
            if file_path and "unittest" not in sys.modules:
                os.remove(file_path)
                print(f"Archivo eliminado: {file_name}")

        # Preguntar al usuario si desea realizar otro proceso
        continuar = input("¿Desea realizar otro proceso? (s/n): ").strip().lower()
        if continuar != 's':
            print("Proceso terminado.")
            break

if __name__ == "__main__":
    main()