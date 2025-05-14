import paho.mqtt.client as mqtt
from pymongo import MongoClient, InsertOne
from datetime import datetime
import time
import json
from queue import Queue
import threading
import atexit

# Config
MAX_QUEUE_SIZE = 1000   # Prevent memory overload
BULK_SIZE = 64          # Documents per bulk insert
FLUSH_INTERVAL = 0.25   # Seconds between bulk inserts
NUM_WRITERS = 32        # Number of MongoDB writer threads

# MQTT Config
MQTT_BROKER = "103.141.231.16"
MQTT_PORT = 1883
# Subscribe to multiple topics
MQTT_TOPICS = ["iot/testing"]

# MongoDB Config
MONGO_URI = "mongodb://WriteHIGO:kejuterbang@103.141.231.17:27017/admin?authSource=admin"
MONGO_DB = "admin"
MONGO_COLLECTION = "data_baru"

# Shared queue for bulk processing
default_queue = Queue(maxsize=MAX_QUEUE_SIZE)
writer_exit = False
client = None


def parse_payload(payload_str):
    data = {}
    items = payload_str.split(',')
    for item in items:
        if '=' not in item:
            continue
        k, v = item.split('=', 1)
        k, v = k.strip(), v.strip()
        
        # Optimized type conversion
        if v.isdigit():
            data[k] = int(v)
        elif v.startswith('-') and v[1:].isdigit():
            data[k] = int(v)
        elif '.' in v:
            try:
                data[k] = float(v)
            except ValueError:
                data[k] = v
        else:
            data[k] = v
    return data


def mongo_writer():
    """Dedicated writer thread for bulk inserts"""
    bulk_buffer = []
    last_flush = time.time()
    try:
        mongo_client = MongoClient(MONGO_URI, maxPoolSize=4)
        db = mongo_client[MONGO_DB]
        collection = db[MONGO_COLLECTION]
        
        while not writer_exit:
            try:
                # Trigger flush if queue has data or timeout reached
                if not default_queue.empty() or (time.time() - last_flush) >= FLUSH_INTERVAL:
                    # Drain queue
                    while True:
                        try:
                            item = default_queue.get_nowait()
                            bulk_buffer.append(InsertOne(item))
                        except:
                            break
                    
                    # Perform bulk write if conditions met
                    if (len(bulk_buffer) >= BULK_SIZE or 
                        (time.time() - last_flush) >= FLUSH_INTERVAL):
                        if bulk_buffer:
                            try:
                                result = collection.bulk_write(bulk_buffer, ordered=False)
                                print(f"[MONGO] Success: Inserted {result.inserted_count} docs, Upserted {result.upserted_count} docs")
                            except Exception as e:
                                print(f"[MONGO] Insert Error: {e}", flush=True)
                            finally:
                                bulk_buffer = []
                                last_flush = time.time()
                # Prevent CPU spin
                time.sleep(0.1)
            except Exception as e:
                print(f"[WRITER] Error: {e}", flush=True)
    except Exception as e:
        print(f"[MONGO CONNECT] Error: {e}", flush=True)
    finally:
        # Final flush on exit
        if bulk_buffer:
            try:
                result = collection.bulk_write(bulk_buffer, ordered=False)
                print(f"[MONGO] Final Flush: Inserted {result.inserted_count} docs, Upserted {result.upserted_count} docs")
            except Exception as e:
                print(f"[MONGO FINAL] Error: {e}", flush=True)


def process_message(payload_str, topic):
    """Minimal processing before queueing"""
    if not payload_str or len(payload_str) > 1024:
        return
    
    try:
        data = parse_payload(payload_str)
        if data:
            data['timestamp'] = datetime.utcnow()
            data.pop('_id', None)
            default_queue.put_nowait(data)
    except Exception as e:
        print(f"[PROC] Error: {str(e)[:100]}", flush=True)


def on_message(mqtt_client, userdata, msg):
    """Direct queue submission"""
    payload = msg.payload.decode().strip()
    process_message(payload, msg.topic)


def shutdown():
    global writer_exit
    print("Shutting down...")
    writer_exit = True
    # Stop MQTT
    if client:
        client.loop_stop()
        client.disconnect()


def main():
    global client
    atexit.register(shutdown)

    # Start multiple writer threads
    for _ in range(NUM_WRITERS):
        threading.Thread(target=mongo_writer, daemon=True).start()

    # MQTT setup
    client = mqtt.Client()
    client.on_message = on_message
    try:
        client.connect(MQTT_BROKER, MQTT_PORT)
        # Subscribe to all configured topics
        for topic in MQTT_TOPICS:
            client.subscribe(topic)
    except Exception as e:
        print(f"[MQTT] Connection Error: {e}", flush=True)
        shutdown()
        return

    client.loop_start()

    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        shutdown()


if __name__ == '__main__':
    main()
