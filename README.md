# IoT Fleet Telemetry Data Lakehouse (MinIO + Delta Lake + PySpark)

Промышленный проект **Data Lakehouse** на базе архитектуры **Medallion (Bronze -> Silver -> Gold)** для обработки телеметрии парка коммерческого транспорта в реальном времени и батч-режиме. Репозиторий демонстрирует построение отказоустойчивого озера данных поверх объектного хранилища **MinIO (S3-compatible)** и открытого табличного формата **Delta Lake** с использованием **Apache Spark (PySpark)**.


## Бизнес-задача и предметная область
Логистический оператор управляет парком из 500+ грузовых автомобилей, оборудованных телематическими датчиками (GPS, расход топлива, температура двигателя, обороты, скорость). Ежедневно датчики генерируют миллионы событий в формате JSON/Parquet с сетевыми задержками, дубликатами и сбоями сенсоров.

**Цели проекта:**
1. Организовать дешёвое масштабируемое хранение сырых логов в S3-совместимом хранилище (**MinIO**) с партиционированием по датам.
2. Обеспечить **ACID-транзакции**, **Schema Enforcement** и **Time Travel** с помощью **Delta Lake**.
3. Реализовать идемпотентную дедупликацию и очистку аномалий сенсоров на уровне **Silver** через `MERGE INTO` (Upsert).
4. Построить витрину предикативного обслуживания и расхода топлива на уровне **Gold** для BI-аналитики и ML.

---

## Архитектура Medallion Lakehouse

```
[ IoT Fleet Sensors (JSON / Parquet) ]
                 │
                 ▼
┌─────────────────────────────────────────┐
│  BRONZE LAYER (Raw S3 / MinIO Bucket)   │  <-- Неизменяемые сырые данные + метаданные загрузки
└────────────────┬────────────────────────┘
                 │ (PySpark: Deduplication, DQ Rules, Schema Enforcement)
                 ▼
┌─────────────────────────────────────────┐
│  SILVER LAYER (Delta Lake Tables)       │  <-- Очищенная телеметрия, ACID MERGE INTO, Z-ORDER
└────────────────┬────────────────────────┘
                 │ (PySpark: Window Functions, Anomaly Detection, KPIs)
                 ▼
┌─────────────────────────────────────────┐
│  GOLD LAYER (Delta Lake Business Marts) │  <-- Агрегаты по рейсам, перерасход топлива, перегрев
└─────────────────────────────────────────┘
```

---

## Технологический стек
* **Object Storage:** MinIO (локальный аналог AWS S3)
* **Table Format:** Delta Lake 3.2 (ACID, Upsert/Merge, Time Travel, Schema Evolution, Z-Ordering)
* **Compute Engine:** Apache Spark 3.5 (PySpark)
* **Infrastructure:** Docker & Docker Compose

---

## Пошаговая инструкция запуска

1. **Запустите MinIO и Spark-окружение:**
   ```bash
   docker-compose up -d --build
   ```
   * Веб-интерфейс MinIO Console будет доступен по адресу: `http://localhost:9001` (логин/пароль: `minioadmin` / `minioadmin`).

2. **Сгенерируйте батч сырой телеметрии (включая дубликаты и сбои датчиков):**
   ```bash
   docker exec -it spark_lakehouse python /app/src/generate_telemetry.py
   ```

3. **Запустите Medallion ETL-пайплайн (Bronze -> Silver -> Gold):**
   ```bash
   docker exec -it spark_lakehouse spark-submit      --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4      /app/src/lakehouse_pipeline.py
   ```

4. **Проверьте Time Travel, историю версий Delta-таблицы и аналитические запросы:**
   ```bash
   docker exec -it spark_lakehouse spark-submit      --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4      /app/src/delta_time_travel_demo.py
   ```
