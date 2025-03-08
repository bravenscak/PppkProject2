import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import gzip
import pandas as pd
from io import BytesIO
from minio import Minio


class TcgaScraper:
    def __init__(self, minio_endpoint, access_key, secret_key):
        self.minio_client = Minio(
            minio_endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=False
        )
        self.bucket_name = "tcga-data"
        self._ensure_bucket_exists()
        self.driver = self._initialize_webdriver()

    def _ensure_bucket_exists(self):
        if not self.minio_client.bucket_exists(self.bucket_name):
            self.minio_client.make_bucket(self.bucket_name)

    def _initialize_webdriver(self):
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        return webdriver.Chrome(options=options)

    def get_cohort_urls(self):
        base_url = "https://xenabrowser.net/datapages/?hub=https://tcga.xenahubs.net:443"
        cohort_data = []

        try:
            self.driver.get(base_url)
            time.sleep(5)
            cohort_elements = self.driver.find_elements(By.CSS_SELECTOR, "a[href*='TCGA']")

            for element in cohort_elements:
                cohort_url = element.get_attribute('href')
                cohort_name = self._extract_cohort_name(cohort_url, element)
                print(f"Processing cohort: {cohort_name}")

                self._open_new_tab(cohort_url)
                download_url = self._find_download_url(cohort_name)
                if download_url:
                    cohort_data.append({'cohort': cohort_name, 'url': download_url})
                self._close_current_tab()

        except Exception as e:
            print(f"Error accessing Xena Browser: {str(e)}")
        finally:
            self.driver.quit()

        return cohort_data

    def _extract_cohort_name(self, cohort_url, element):
        cohort_parts = cohort_url.split('TCGA.')
        if len(cohort_parts) > 1:
            return cohort_parts[1].split('.')[0]
        return element.text.strip()

    def _open_new_tab(self, url):
        self.driver.execute_script(f"window.open('{url}', '_blank')")
        self.driver.switch_to.window(self.driver.window_handles[-1])
        time.sleep(3)

    def _find_download_url(self, cohort_name):
        try:
            link = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.XPATH,
                                                "//a[contains(text(), 'IlluminaHiSeq') and contains(text(), 'pancan normalized')]"))
            )
            link.click()
            time.sleep(2)
            download_link = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='download']"))
            )
            return download_link.get_attribute('href')
        except Exception:
            print(f"No requested data found for {cohort_name}")
            return None

    def _close_current_tab(self):
        self.driver.close()
        self.driver.switch_to.window(self.driver.window_handles[0])

    def process_gene_expression(self, data, genes_of_interest):
        try:
            df = pd.read_csv(BytesIO(data), sep='\t')
            df_transposed = df.transpose()
            df_transposed.columns = df_transposed.iloc[0]
            df_transposed = df_transposed.iloc[1:]

            genes_found = [gene for gene in genes_of_interest if gene in df_transposed.columns]
            if not genes_found:
                return []

            return df_transposed[genes_found].to_dict(orient='records')
        except Exception:
            return []

    def process_and_upload_cohort(self, url, cohort_name, genes_of_interest):
        try:
            print(f"Processing cohort: {cohort_name}")
            response = requests.get(url, stream=True, timeout=60)
            response.raise_for_status()

            decompressed_content = gzip.decompress(response.content)
            processed_data = self.process_gene_expression(decompressed_content, genes_of_interest)

            normalized_cohort_name = ''.join(c for c in cohort_name if c.isalnum() or c in [' ', '_']).rstrip()
            minio_filename = f"{normalized_cohort_name}_gene_expression.tsv"

            print(f"Uploading {minio_filename} to MinIO")
            self.minio_client.put_object(
                bucket_name=self.bucket_name,
                object_name=minio_filename,
                data=BytesIO(decompressed_content),
                length=len(decompressed_content)
            )

            print(f"Successfully processed and uploaded {minio_filename}")
            return processed_data
        except Exception as e:
            print(f"Error processing cohort {cohort_name}: {str(e)}")
            raise


def main():
    minio_endpoint = "localhost:9000"
    access_key = "yourMinioAccessKey"
    secret_key = "yourMinioPassword"

    genes_of_interest = [
        'C6orf150', 'CCL5', 'CXCL10', 'TMEM173',
        'CXCL9', 'CXCL11', 'NFKB1', 'IKBKE', 'IRF3',
        'TREX1', 'ATM', 'IL6', 'CXCL8'
    ]

    scraper = TcgaScraper(minio_endpoint, access_key, secret_key)
    cohort_data = scraper.get_cohort_urls()

    all_processed_data = []
    for cohort in cohort_data:
        try:
            processed_data = scraper.process_and_upload_cohort(
                cohort['url'],
                cohort['cohort'],
                genes_of_interest
            )
            all_processed_data.extend(processed_data)
            print(f"Successfully processed {len(processed_data)} records for {cohort['cohort']}")
        except Exception as e:
            print(f"Error processing cohort {cohort['cohort']}: {str(e)}")
            continue

    print(f"Total processed records: {len(all_processed_data)}")


if __name__ == "__main__":
    main()
