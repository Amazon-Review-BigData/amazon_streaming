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


## Adım 5: Özellik Mühendisliği (Feature Engineering)

1. Silver katmanındaki temiz verinin `/opt/spark-apps/output/processed/amazon_reviews` altında hazır olduğundan emin olun.
2. Feature engineering scriptini Spark container içinde çalıştırın.

Örnek:

```bash
docker exec -it big_data_spark_master /opt/spark/bin/spark-submit /opt/spark-apps/feature_engineering.py
```

Gold feature tablosu şu klasöre Delta formatında yazılır:

```bash
/opt/spark-apps/output/processed/gold_features
```

Üretilen özellikler:

- `review_word_count`
- `helpfulness_ratio`
- `headline_length`
- `is_verified`
- `is_long_review`

Bu aşamada `star_rating` hedef değişken olarak korunur.

review_word_count (Yorum Kelime Sayısı)

Nasıl Hesaplanır: review_body sütunundaki metin, boşluklara göre parçalanır (tokenization) ve kelime adedi sayılır.

İş Mantığı: Tüketici psikolojisine göre, bir ürün hakkında çok detaylı bilgi veren kullanıcılar daha yüksek kelime sayısına ulaşır. Çok kısa yorumlar (örn: "OK", "Good") genellikle düşük bilgi değerine sahipken, uzun yorumlar ürünün hem artılarını hem eksilerini içerdiği için modelin ürün kalitesini daha hassas ölçmesine yardımcı olur.  

2. helpfulness_ratio (Yardımcı Olma Oranı)

Nasıl Hesaplanır: helpful_votes değerinin total_votes değerine bölünmesiyle elde edilir (Ratio= 
Total
Helpful). Toplam oy sıfır ise sonuç 0 olarak atanır.

İş Mantığı: Bu özellik bir "sosyal kanıt" (social proof) sinyalidir. Diğer alıcıların bir yorumu "faydalı" olarak işaretlemesi, o yorumdaki bilginin doğruluğunu ve değerini onaylar. Bu oran 1.0'a yaklaştıkça, o satırdaki verinin (yıldız puanı ve metin) güvenilirliği artar.  

3. headline_length (Başlık Uzunluğu)

Nasıl Hesaplanır: review_headline (yorum başlığı) kısmındaki karakter sayısıdır.

İş Mantığı: Başlıklar, kullanıcının ürüne dair ilk ve en güçlü tepkisini yansıtır. Çok kısa başlıklar (örn: "!") veya çok uzun başlıklar, kullanıcının o anki duygusal durumunun (çok mutlu veya çok öfkeli) bir göstergesi olabilir. Bu da duygu analizi modelleri için önemli bir değişkendir.  

4. is_verified (Doğrulanmış Alıcı)

Nasıl Hesaplanır: verified_purchase sütunundaki 'Y' (Evet) değeri 1'e, 'N' (Hayır) değeri 0'a dönüştürülür.

İş Mantığı: Amazon ekosisteminde "Verified Purchase" etiketi, yorumu yapan kişinin ürünü gerçekten o platformdan satın aldığını kanıtlar. Doğrulanmış yorumların, manipülatif veya sahte yorum olma ihtimali daha düşüktür; bu nedenle modelin bu veriye daha yüksek ağırlık vermesi istenir.  

5. is_long_review (Uzun Yorum Bayrağı)

Nasıl Hesaplanır: Tüm veri setindeki ortalama kelime sayısı hesaplanır. Eğer mevcut satırdaki review_word_count bu ortalamanın üzerindeyse bu sütun 1, altındaysa 0 değerini alır (Binary Feature).

İş Mantığı: Modeli karmaşıklıktan kurtarmak için veriyi kategorize eder. "Sıradan yorumlar" ile "kapsamlı incelemeleri" birbirinden ayırır. Bu ayrım, özellikle ürünün uzun vadeli dayanıklılığı veya teknik detayları hakkında tahminleme yaparken modelin odaklanacağı veri kümesini netleştirir.

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

## Adım 6: Makine Öğrenmesi - Çoklu Model Karşılaştırma

1. Adım 5 sonunda oluşan Gold Delta tablosunun `/opt/spark-apps/output/processed/gold_features` altında hazır olduğundan emin olun.
2. Eğitim scriptini Spark container içinde çalıştırın.

Örnek:

```bash
docker exec -it big_data_spark_master /opt/spark/bin/spark-submit /opt/spark-apps/ml_training.py
```

Bu adımda şu modeller karşılaştırılır:

- Logistic Regression
- Decision Tree
- Random Forest
- GBT (Gradient Boosted Trees)
- Naive Bayes

Her model için MLflow üzerinde ayrı bir run açılır ve şu metrikler kaydedilir:

- Accuracy
- F1-Score
- Precision
- Recall
- AUC-ROC

Ek olarak her model için şu artifact'ler loglanır:

- Confusion Matrix grafiği
- Feature Importance analizi

En iyi model, F1-Score öncelikli olacak şekilde seçilir ve `amazon_reviews_best_model` adıyla MLflow Model Registry'ye kaydedilir.

---