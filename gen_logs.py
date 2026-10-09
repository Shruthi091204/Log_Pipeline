import time
import random
import datetime

LOG_FILE = "test.log"
print(f"Generating fake logs to {LOG_FILE}...")

def generate_logs():
    with open(LOG_FILE, "a") as f:
        while True:
            # 5% chance to have a massive error spike
            is_spike = random.random() < 0.05
            num_logs = random.randint(50, 100) if is_spike else random.randint(1, 5)
            
            for _ in range(num_logs):
                # During a spike, 80% of logs are ERRORs
                if is_spike and random.random() < 0.8:
                    level = "ERROR"
                else:
                    level = random.choice(["INFO", "DEBUG", "WARN"])
                
                msg = f"{datetime.datetime.now().isoformat()} [{level}] System event recorded.\n"
                f.write(msg)
                
            f.flush()
            time.sleep(1)

if __name__ == "__main__":
    generate_logs()
