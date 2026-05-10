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

1. Önce streaming adımının `/opt/spark-apps/output/processed/amazon_reviews` klasörüne veri yazdığından emin olun.
2. Spark container'ı içinde EDA scriptini çalıştırın.

Örnek:

```bash
docker exec -it big_data_spark_master /opt/spark/bin/spark-submit /opt/spark-apps/eda.py
```

EDA çıktıları şu klasöre yazılır:

```bash
/opt/spark-apps/output/eda_outputs
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


---

# 🧪 Farklı Veri Setleri ile Test Etme (Opsiyonel)

Sistem, Amazon US Reviews veri setindeki tüm kategorilerle uyumlu çalışacak şekilde tasarlanmıştır. Başka bir veri setiyle (örneğin: `Digital_Software`) test etmek için şu adımları izleyin:

1.  **Sistemi Durdurun:** Mevcut çalışan konteynerleri kapatın:
    ```bash
    docker compose down
    ```

2.  **Veriyi Hazırlayın:** Yeni `.tsv` dosyasını `data/` klasörüne kopyalayın.

3.  **Geçmişi Temizleyin:** Eski verilerin karışmaması için şu dizinlerin **içindekileri** silin (klasörler kalsın):
    * `data/checkpoints/`
    * `spark/output/processed/`
    * `spark/output/eda_outputs/`

4.  **Yapılandırmayı Güncelleyin:** `docker-compose.yml` dosyasındaki `python-producer` servisi altında yer alan `DATA_FILE` yolunu yeni dosya adıyla değiştirin:
    ```yaml
    environment:
      DATA_FILE: /data/amazon_reviews_us_Digital_Software_v1_00.tsv
    ```

5.  **Sistemi Ateşleyin:**
    ```bash
    # Konteynerleri başlatın
    docker compose up -d

    # Spark Streaming akışını başlatın
    docker exec -u root -it big_data_spark_master /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /opt/spark-apps/streaming.py
    ```

6.  **Görselleştirme:** * Veri akışı bittikten sonra `eda.py`'yi çalıştırın:
      ```bash
      docker exec -u root -it big_data_spark_master /opt/spark/bin/spark-submit /opt/spark-apps/eda.py
      ```
    * Ardından Jupyter Notebook (`eda_visualize.ipynb`) üzerinden **"Run All"** yaparak yeni grafiklerinizi saniyeler içinde görüntüleyin.

---