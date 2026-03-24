import boto3
import requests
import csv
import logging
import concurrent.futures
import threading
import io
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

TARGET_REGIONS = [
    'US East (N. Virginia)', 'US East (Ohio)', 'US West (N. California)', 
    'US West (Oregon)', 'South America (Sao Paulo)', 'EU (Ireland)', 
    'Asia Pacific (Sydney)', 'Global', 'Any'
]

S3_BUCKET = 'price-list-aws'
# Destino isolado no S3: Pasta split_csvs_bulk 
S3_KEY = 'split_csvs_bulk/aws_prices_api.csv'
CSV_HEADERS = ['SKU', 'Service Name', 'Location', 'Public List Price', 'Price Unit', 'Description']

INDEX_URL = "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/index.json"
BASE_URL = "https://pricing.us-east-1.amazonaws.com"

class S3MultipartCSVWriter:
    """Ferramenta remota do S3 conectada na sua conta corporativa segura."""
    def __init__(self, bucket, key):
        self.session = boto3.Session(profile_name='Core-AWSAdministratorAccess-047303556943')
        self.s3 = self.session.client('s3')
        self.bucket = bucket
        self.key = key
        self._ensure_bucket_exists()
        
        self.upload_id = self.s3.create_multipart_upload(Bucket=self.bucket, Key=self.key)['UploadId']
        self.parts = []
        self.part_number = 1
        self.buffer = io.StringIO()
        self.lock = threading.Lock()
        
    def _ensure_bucket_exists(self):
        try:
            self.s3.head_bucket(Bucket=self.bucket)
        except ClientError as e:
            error_code = int(e.response['Error']['Code']) if 'Error' in e.response else 404
            if error_code == 404:
                region = self.session.region_name or 'us-east-1'
                if region == 'us-east-1':
                    self.s3.create_bucket(Bucket=self.bucket)
                else:
                    self.s3.create_bucket(Bucket=self.bucket, CreateBucketConfiguration={'LocationConstraint': region})
            else:
                raise e

    def write(self, data):
        with self.lock:
            self.buffer.write(data)
            if self.buffer.tell() > (10 * 1024 * 1024):
                self._flush_part()
                
    def _flush_part(self):
        body = self.buffer.getvalue().encode('utf-8')
        if not body: return
            
        res = self.s3.upload_part(
            Bucket=self.bucket, Key=self.key, PartNumber=self.part_number,
            UploadId=self.upload_id, Body=body
        )
        self.parts.append({'PartNumber': self.part_number, 'ETag': res['ETag']})
        logging.info(f"[{self.bucket}] Bloco {self.part_number} ({(len(body)/(1024*1024)):.2f} MB) transferido do arquivo {self.key}.")
        self.part_number += 1
        self.buffer.close()
        self.buffer = io.StringIO()

    def close(self):
        with self.lock:
            if self.buffer.tell() > 0:
                self._flush_part()
            if self.parts:
                self.s3.complete_multipart_upload(Bucket=self.bucket, Key=self.key, UploadId=self.upload_id, MultipartUpload={'Parts': self.parts})
                logging.info(f"Arquivo CSV finalizado no S3: s3://{self.bucket}/{self.key}")
            else:
                self.s3.abort_multipart_upload(Bucket=self.bucket, Key=self.key, UploadId=self.upload_id)

    def __enter__(self): return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.s3.abort_multipart_upload(Bucket=self.bucket, Key=self.key, UploadId=self.upload_id)
        else:
            self.close()

class S3ChunkedCSVWriter:
    """Orquestrador que quebra o CSV automaticamente em novos arquivos antes de atingir
    o limite máximo de linhas suportado pelo Microsoft Excel (1 milhão)."""
    def __init__(self, bucket, base_key, max_rows=900000):
        self.bucket = bucket
        self.base_key = base_key
        self.max_rows = max_rows
        self.lock = threading.Lock()
        
        self.current_part = 1
        self.current_rows = 0
        self.active_writer = None
        self._rollover()
        
    def _rollover(self):
        if self.active_writer:
            self.active_writer.close()
            self.current_part += 1
            
        new_key = self.base_key.replace('.csv', f'_parte_{self.current_part}.csv')
        self.active_writer = S3MultipartCSVWriter(self.bucket, new_key)
        
        # Escrever o cabeçalho no topo do novo arquivo gerado
        virtual_header = io.StringIO(newline='')
        header_writer = csv.DictWriter(virtual_header, fieldnames=CSV_HEADERS)
        header_writer.writeheader()
        self.active_writer.write(virtual_header.getvalue())
        self.current_rows = 0
        logging.info(f"==> Abriu um NOVO arquivo Excel-Safe no S3: s3://{self.bucket}/{new_key}")

    def write_rows(self, data_str, num_rows):
        with self.lock:
            # Rotação segura de arquivo se for estourar o limite de linhas
            if self.current_rows + num_rows > self.max_rows and self.current_rows > 0:
                self._rollover()
                
            self.active_writer.write(data_str)
            self.current_rows += num_rows

    def close(self):
        with self.lock:
            if self.active_writer:
                self.active_writer.close()
                
    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        with self.lock:
            if exc_type is not None and self.active_writer:
                logging.error("Interrompendo Chunk atual do S3 devido a um erro.")
                self.active_writer.s3.abort_multipart_upload(
                    Bucket=self.active_writer.bucket, 
                    Key=self.active_writer.key, 
                    UploadId=self.active_writer.upload_id
                )
            elif self.active_writer:
                self.active_writer.close()

def process_bulk_service(service_code, csv_url, chunked_writer):
    """Baixa o CSV via HTTP Streaming (Express Bulk API), filtra dados irrelevantes e empacota no S3 velozmente."""
    try:
        with requests.get(csv_url, stream=True) as r:
            r.raise_for_status()
            lines = (line.decode('utf-8') for line in r.iter_lines() if line)
            
            # Pula as 5 linhas de metadados inúteis do arquivo oficial da AWS
            for _ in range(5): next(lines, None) 
            
            reader = csv.reader(lines)
            headers = next(reader, None)
            if not headers: return
            
            # Mapeia colunas dinamicamente (para prever alterações futuras da AWS)
            col_map = {
                'SKU': headers.index('SKU') if 'SKU' in headers else -1,
                'Location': headers.index('Location') if 'Location' in headers else -1,
                'Price': headers.index('PricePerUnit') if 'PricePerUnit' in headers else -1,
                'Unit': headers.index('Unit') if 'Unit' in headers else -1,
                'Desc': headers.index('Description') if 'Description' in headers else -1
            }
            
            virtual_file = io.StringIO(newline='')
            writer = csv.writer(virtual_file)
            count = 0
            
            for row in reader:
                loc = row[col_map['Location']] if col_map['Location'] != -1 and len(row) > col_map['Location'] else 'Global'
                
                # AWS Bulk API costuma vir com local igual nossas tags ("US East (N. Virginia)", etc). 
                # Bate no filtro para descartar dados indesejados!
                if loc in TARGET_REGIONS:
                    sku = row[col_map['SKU']] if col_map['SKU'] != -1 and len(row) > col_map['SKU'] else ''
                    price = row[col_map['Price']] if col_map['Price'] != -1 and len(row) > col_map['Price'] else ''
                    
                    # [BUGFIX EXCEL PT-BR] A AWS devolve ponto (1.50) e nosso Windows/Excel no Brasil espera vírgula (1,50). 
                    # Trocar o separador no ar salva o arquivo de bugar e contar como Bilhões.
                    if price:
                        price = price.replace('.', ',')
                        
                    unit = row[col_map['Unit']] if col_map['Unit'] != -1 and len(row) > col_map['Unit'] else ''
                    desc = row[col_map['Desc']] if col_map['Desc'] != -1 and len(row) > col_map['Desc'] else ''
                    
                    writer.writerow([sku, service_code, loc, price, unit, desc])
                    count += 1
                    
                    # Descarrega na RAM central periodicamente (a cada 10000 linhas purificadas) para evitar overflow do computador
                    if count % 10000 == 0:
                        chunked_writer.write_rows(virtual_file.getvalue(), 10000)
                        virtual_file = io.StringIO(newline='')
                        writer = csv.writer(virtual_file)
                        
            # Salvar as linhas finais remanescentes fora da quebra inteira de 10000
            remainder = count % 10000
            if remainder > 0:
                chunked_writer.write_rows(virtual_file.getvalue(), remainder)
                
            if count > 0:
                logging.info(f"[+] {count} preços cruzados e filtrados do serviço **{service_code}** enviados diretamente pro cofre (AWS Bulk).")
                
    except Exception as e:
        logging.error(f"Erro ao processar express {service_code}: {e}")

def main():
    logging.info("Acessando arquivo-mestre da AWS Bulk API Expressa...")
    try:
        index_data = requests.get(INDEX_URL).json()
    except Exception as e:
        logging.error(f"Falha ao conectar via HTTP na AWS Bulk API. Tem o pacote 'requests' instalado? Erro: {e}")
        return
        
    offers = index_data['offers']
    logging.info(f"Sucesso. Identificados {len(offers)} serviços globais de faturamento da AWS.")
    
    # Orquestra a divisao em arquivos parte 1, parte 2 (maximo de 900.000 linhas garantido)
    with S3ChunkedCSVWriter(bucket=S3_BUCKET, base_key=S3_KEY, max_rows=900000) as chunked_writer:
        
        # Restringe para 5 processos rápidos pra não derrubar a própria banda de internet!
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            for service_code, service_info in offers.items():
                # O script de base da AWS que você enviou estava usando 'currentRegionIndexUrl', o que causava o Erro 404 (pois a AWS não mapeia arquivos CSV estáticos por região neste endpoint).
                # A forma oficial correta da Bulk API de puxar o CSV de faturamento com todas as regiões integradas é pela chave 'currentVersionUrl'!
                csv_url = BASE_URL + service_info['currentVersionUrl'].replace('.json', '.csv')
                futures.append(executor.submit(process_bulk_service, service_code, csv_url, chunked_writer))
                
            concurrent.futures.wait(futures)
            
    logging.info("Extração Expressa 100% FINALIZADA! Seus relatórios separados já lhe aguardam no S3 (split_csvs_bulk).")

if __name__ == '__main__':
    main()
