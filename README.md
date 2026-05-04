# amazon_streaming

Docker tabanlı büyük veri altyapısı için minimal başlangıç deposu.

## İçerik

- `docker-compose.yml`: ZooKeeper, Kafka, Spark Master/Worker ve producer konteynerleri

## Çalıştırma

```bash
docker compose up -d
```

## Not

Bu depo sadece altyapı tarafını içerir; veri işleme ve modelleme bileşenleri bu sürüme dahil edilmemiştir.
