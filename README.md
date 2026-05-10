# amazon_streaming

Docker tabanlı Amazon review büyük veri pipeline'i.

## İçerik

- `docker-compose.yml`: ZooKeeper, Kafka, Spark Master/Worker ve producer konteynerleri
- `kafka_producer.py`: TSV verisini Kafka'ya gönderen üretici
- `spark/streaming.py`: Kafka'dan veri okuyup temizleyen Spark Structured Streaming adımı
- `spark/eda.py`: Temizlenmiş veriden EDA özetleri üreten Spark batch adımı

## Çalıştırma

```bash
docker compose up -d
```

## Adım 4: Keşifsel Veri Analizi (EDA)

1. Önce streaming adımının ` /data/processed/amazon_reviews ` klasörüne veri yazdığından emin olun.
2. Spark container'ı içinde EDA scriptini çalıştırın.

Örnek:

```bash
docker exec -it big_data_spark_master /opt/spark/bin/spark-submit /opt/spark-apps/eda.py
```

EDA çıktıları şu klasöre yazılır:

```bash
/data/eda_outputs
```

Bu adımda üretilen özetler:

- toplam kayıt sayısı
- benzersiz review/product/customer sayıları
- sayısal alanlar için `describe()` istatistikleri
- eksik değer sayıları
- yıldız puanı dağılımı
- verified purchase ve vine dağılımları
- aylık review trendi
- en sık review alan ürünler ve müşteriler
- rating bazlı ortalama helpful/total votes ve review length
