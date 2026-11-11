import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

def create_spark_session() -> SparkSession:
    s3_endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

    return (
        SparkSession.builder
        .appName("IoT_Fleet_Medallion_Lakehouse")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )

def run_medallion_pipeline():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    raw_input_path = "/app/data/raw_telemetry_batch.json"
    bronze_path = "s3a://lakehouse/bronze/fleet_telemetry"
    silver_path = "s3a://lakehouse/silver/fleet_telemetry_clean"
    gold_path = "s3a://lakehouse/gold/fleet_daily_kpis"

    # =========================================================================
    # 1. BRONZE LAYER: Raw Ingestion с сохранением метаданных аудита
    # =========================================================================
    print("--- [1/3] Ingesting Raw Telemetry into Bronze Layer ---")
    df_raw = (
        spark.read.json(raw_input_path)
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source_file", F.input_file_name())
        .withColumn("event_date", F.to_date("event_timestamp"))
    )

    (
        df_raw.write
        .format("delta")
        .mode("append")
        .partitionBy("event_date")
        .save(bronze_path)
    )
    print(f"Bronze layer updated. Total rows ingested in batch: {df_raw.count()}")

    # =========================================================================
    # 2. SILVER LAYER: Data Quality, Deduplication & ACID MERGE INTO
    # =========================================================================
    print("--- [2/3] Transforming & Upserting into Silver Layer ---")
    df_bronze = spark.read.format("delta").load(bronze_path)

    # Правила Data Quality (отсекаем физически невозможные значения датчиков)
    df_dq_valid = df_bronze.filter(
        (F.col("event_id").isNotNull()) &
        (F.col("vehicle_id").isNotNull()) &
        (F.col("speed_kmh") >= 0.0) & (F.col("speed_kmh") <= 160.0) &
        (F.col("fuel_rate_lph") >= 0.0) &
        (F.col("engine_temp_c").between(-40.0, 150.0))
    )

    # Дедупликация по event_id (оставляем самую свежую запись по ingestion_timestamp)
    dedup_window = Window.partitionBy("event_id").orderBy(F.col("ingestion_timestamp").desc())
    df_silver_staged = (
        df_dq_valid
        .withColumn("rn", F.row_number().over(dedup_window))
        .filter(F.col("rn") == 1)
        .drop("rn", "source_file")
        .withColumn("event_ts", F.to_timestamp("event_timestamp"))
        .withColumn("is_overheating", F.when(F.col("engine_temp_c") > 105.0, True).otherwise(False))
        .withColumn("processed_at", F.current_timestamp())
    )

    # Идемпотентный ACID MERGE INTO (Upsert) в Delta-таблицу Silver
    if DeltaTable.isDeltaTable(spark, silver_path):
        silver_table = DeltaTable.forPath(spark, silver_path)
        (
            silver_table.alias("target")
            .merge(
                df_silver_staged.alias("source"),
                "target.event_id = source.event_id AND target.event_date = source.event_date"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        (
            df_silver_staged.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("event_date")
            .save(silver_path)
        )

    # Оптимизация хранения (Compaction + Z-Ordering по vehicle_id)
    spark.sql(f"OPTIMIZE delta.`{silver_path}` ZORDER BY (vehicle_id)")
    print("Silver layer successfully merged and Z-Ordered.")

    # =========================================================================
    # 3. GOLD LAYER: Агрегация бизнес-метрик для аналитики и предикативного ТО
    # =========================================================================
    print("--- [3/3] Building Gold Business Mart ---")
    df_silver = spark.read.format("delta").load(silver_path)

    df_gold = (
        df_silver
        .groupBy("event_date", "vehicle_id", "route_code")
        .agg(
            F.count("event_id").alias("telemetry_points_count"),
            F.round(F.avg("speed_kmh"), 2).alias("avg_speed_kmh"),
            F.round(F.max("speed_kmh"), 2).alias("max_speed_kmh"),
            F.round(F.avg("fuel_rate_lph"), 2).alias("avg_fuel_rate_lph"),
            F.round(F.max("engine_temp_c"), 1).alias("max_engine_temp_c"),
            F.sum(F.when(F.col("is_overheating"), 1).otherwise(0)).alias("overheating_events_count")
        )
        .withColumn(
            "maintenance_risk_level",
            F.when(F.col("overheating_events_count") >= 3, "HIGH_RISK")
            .when(F.col("overheating_events_count") >= 1, "MEDIUM_RISK")
            .otherwise("NORMAL")
        )
        .withColumn("updated_at", F.current_timestamp())
    )

    (
        df_gold.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("event_date")
        .save(gold_path)
    )
    print("Gold layer refreshed. Sample KPI output:")
    df_gold.orderBy(F.col("overheating_events_count").desc()).show(10, truncate=False)

    spark.stop()

if __name__ == "__main__":
    run_medallion_pipeline()