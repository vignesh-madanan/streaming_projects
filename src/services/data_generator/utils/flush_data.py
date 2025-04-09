import psycopg2
from dotenv import load_dotenv
import logging
import os
from confluent_kafka.admin import AdminClient, ConfigResource, RESOURCE_TOPIC
import time
import concurrent.futures
load_dotenv(".env")
logging.basicConfig(level=logging.INFO)
# Database environment variables
DB_NAME = os.getenv("DB_NAME", "data_generator")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
DB_HOST = os.getenv("DB_HOST", "localhost")

# Kafka environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

def drop_database():
    # Drop database if it exists
    conn = psycopg2.connect(
        dbname="postgres",
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
    )
    conn.autocommit = True
    curr = conn.cursor()
    curr.execute(f"DROP DATABASE IF EXISTS {DB_NAME}")
    curr.close()
    conn.close()

def truncate_database(db_names: list[str]):
    # Truncate commerce.users and commerce.products tables
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
    )
    curr = conn.cursor()
    curr.execute("TRUNCATE {', '.join(db_names};")
    curr.close()
    conn.close()

def flush_kafka_queue(topic_name):
    # Set up the admin client
    admin_client = AdminClient({
        'bootstrap.servers': 'localhost:9092'
    })

    # Step 1: Set retention.ms to 0 (simulate truncation)
    resource = ConfigResource(RESOURCE_TOPIC, topic_name)
    futures = admin_client.alter_configs([ConfigResource(RESOURCE_TOPIC, topic_name, set_config={"retention.ms": "0"})])

    # Wait for result
    for res, f in futures.items():
        try:
            f.result()
            print(f"Retention set to 0 for topic '{topic_name}'")
        except Exception as e:
            print(f"Failed to update config for {res.name}: {e}")

    # Step 2: Wait for Kafka log cleaner to clean up messages
    time.sleep(3)  # tweak as needed

    # Step 3: Reset retention.ms to default (7 days = 604800000)
    futures = admin_client.alter_configs([ConfigResource(RESOURCE_TOPIC, topic_name, set_config={"retention.ms": "604800000"})])

    for res, f in futures.items():
        try:
            f.result()
            print(f"Retention reset to default for topic '{topic_name}'")
        except Exception as e:
            print(f"Failed to reset config for {res.name}: {e}")

if __name__ == "__main__":
    truncate_database(["commerce.users", "commerce.products"])
    flush_kafka_queue("clicks")
    flush_kafka_queue("checkouts")