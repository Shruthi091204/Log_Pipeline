import os
import time
import json
import boto3
import numpy as np
from kafka import KafkaConsumer

KAFKA_BROKERS = os.getenv('KAFKA_BROKERS', 'localhost:9092')
BUCKET = "my-log-bucket"
S3_ENDPOINT = os.getenv('S3_ENDPOINT', None) # Local testing support

consumer = KafkaConsumer('logs', bootstrap_servers=KAFKA_BROKERS, auto_offset_reset='latest')
s3 = boto3.client('s3', endpoint_url=S3_ENDPOINT)

current_errors = 0
history = []
raw_lines = []
last_min_ts = time.time()
last_5min_ts = time.time()

print(f"Consumer listening on {KAFKA_BROKERS} for topic 'logs'...")

for msg in consumer:
    line = msg.value.decode('utf-8')
    raw_lines.append(line)
    if "ERROR" in line:
        current_errors += 1
    
    now = time.time()
    
    # 1-minute window: anomaly detection (z-score)
    if now - last_min_ts >= 60:
        history.append(current_errors)
        if len(history) > 5:
            mean = np.mean(history[:-1])
            std = np.std(history[:-1])
            z_score = (current_errors - mean) / (std + 1e-5)
            if z_score > 2:
                print(f"[ALERT] Anomalous error spike! {current_errors} errors (Z={z_score:.2f})")
            else:
                print(f"[INFO] 1-min errors: {current_errors}")
        else:
            print(f"[INFO] 1-min errors (warming up): {current_errors}")
            
        current_errors = 0
        last_min_ts = now
    
    # 5-minute window: upload to S3
    if now - last_5min_ts >= 300:
        if raw_lines:
            file_name = f"logs_{int(now)}.txt"
            try:
                with open(file_name, "w") as f:
                    f.writelines(raw_lines)
                s3.upload_file(file_name, BUCKET, file_name)
                print(f"[S3] Uploaded {len(raw_lines)} lines to s3://{BUCKET}/{file_name}")
            except Exception as e:
                print(f"[S3 ERROR] Upload failed: {e}")
            finally:
                if os.path.exists(file_name):
                    os.remove(file_name)
            raw_lines = []
        last_5min_ts = now
