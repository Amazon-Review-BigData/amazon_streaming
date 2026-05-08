#!/usr/bin/env python
"""
Kafka Producer - Amazon Reviews TSV verisini Kafka'ya gönderir
"""

import os
import time
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging

# Logging yapılandırması
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Ortam değişkenlerinden konfigürasyon al
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:29092").split(",")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "amazon_reviews")
DATA_FILE = os.getenv("DATA_FILE", "/data/amazon_reviews_us_Mobile_Electronics_v1_00.tsv")
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "50000"))
BATCH_DELAY = float(os.getenv("BATCH_DELAY", "0.5"))

def create_kafka_producer():
    """Kafka producer oluştur"""
    max_retries = 30
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            logger.info(f"Kafka'ya bağlanılıyor: {KAFKA_BROKERS}")
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3,
                max_in_flight_requests_per_connection=1,
                compression_type='gzip'
            )
            logger.info("Kafka producer başarıyla oluşturuldu")
            return producer
        except Exception as e:
            retry_count += 1
            logger.warning(f"Bağlantı denemi {retry_count}/{max_retries} başarısız: {str(e)}")
            if retry_count < max_retries:
                time.sleep(2)
            else:
                logger.error("Kafka'ya bağlanılamadı")
                raise

def send_data_to_kafka(producer):
    """TSV dosyasından verileri Kafka'ya gönder"""
    if not os.path.exists(DATA_FILE):
        logger.error(f"Veri dosyası bulunamadı: {DATA_FILE}")
        return

    try:
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            # TSV header
            headers = f.readline().strip().split('\t')
            logger.info(f"TSV başlıkları: {headers}")
            
            batch_count = 0
            record_count = 0
            
            for line in f:
                try:
                    values = line.strip().split('\t')
                    
                    if len(values) != len(headers):
                        logger.warning(f"Satır sütun sayısı eşleşmiyor, atlanıyor: {values[:3]}")
                        continue
                    
                    # Sözlüğe dönüştür
                    record = {headers[i]: values[i] for i in range(len(headers))}
                    
                    # Kafka'ya gönder
                    future = producer.send(KAFKA_TOPIC, value=record)
                    future.get(timeout=10)  # Gönderimin tamamlanmasını bekle
                    
                    record_count += 1
                    batch_count += 1
                    
                    if batch_count >= CHUNK_SIZE:
                        logger.info(f"Toplam {record_count} kayıt gönderildi")
                        batch_count = 0
                        time.sleep(BATCH_DELAY)
                
                except KafkaError as e:
                    logger.error(f"Kafka gönderimi hatası: {str(e)}")
                    raise
                except Exception as e:
                    logger.error(f"Kayıt işleme hatası: {str(e)}")
                    continue
            
            logger.info(f"Veri gönderimi tamamlandı. Toplam kayıt: {record_count}")
            producer.flush()
            
    except Exception as e:
        logger.error(f"Dosya okuma hatası: {str(e)}")
        raise

def main():
    """Ana fonksiyon"""
    logger.info("Kafka Producer başlıyor...")
    
    producer = create_kafka_producer()
    try:
        send_data_to_kafka(producer)
    finally:
        producer.close()
        logger.info("Producer kapatıldı")

if __name__ == "__main__":
    main()
