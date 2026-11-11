import json
import os
import random
from datetime import datetime, timedelta

def generate_iot_batch(output_path: str = "/app/data/raw_telemetry_batch.json", num_records: int = 5000):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    vehicles = [f"TRUCK-{i:03d}" for i in range(1, 51)]
    routes = ["MSK-SPB", "MSK-KZN", "SPB-EKB", "KZN-NSK", "MSK-RND"]
    base_time = datetime(2026, 11, 11, 8, 0, 0)
    
    records = []
    for idx in range(num_records):
        vehicle_id = random.choice(vehicles)
        event_ts = base_time + timedelta(seconds=random.randint(0, 86400))
        
        speed_kmh = round(random.uniform(0.0, 95.0), 1)
        fuel_rate_lph = round(18.0 + (speed_kmh * 0.22) + random.uniform(-2.0, 4.0), 2)
        engine_temp_c = round(random.uniform(75.0, 98.0), 1)
        
        # Имитация аномалий и сбоев IoT-датчиков (5% записей)
        if random.random() < 0.03:
            engine_temp_c = round(random.uniform(108.0, 125.0), 1)  # Реальный перегрев двигателя
        if random.random() < 0.02:
            speed_kmh = -15.0  # Битый пакет датчика скорости (должен быть отфильтрован в Silver)
            
        record = {
            "event_id": f"EVT-{100000 + idx}",
            "vehicle_id": vehicle_id,
            "route_code": random.choice(routes),
            "event_timestamp": event_ts.strftime("%Y-%m-%d %H:%M:%S"),
            "speed_kmh": speed_kmh,
            "fuel_rate_lph": fuel_rate_lph,
            "engine_temp_c": engine_temp_c,
            "latitude": round(random.uniform(55.0, 60.0), 5),
            "longitude": round(random.uniform(30.0, 49.0), 5)
        }
        records.append(record)
        
        # Имитация сетевых дубликатов (at-least-once delivery от IoT-шлюза)
        if random.random() < 0.08:
            records.append(record.copy())

    with open(output_path, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
            
    print(f"Generated {len(records)} telemetry events (including duplicates and sensor glitches) at {output_path}")

if __name__ == "__main__":
    generate_iot_batch()