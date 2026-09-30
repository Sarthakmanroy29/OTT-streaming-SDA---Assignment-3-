import json
import time
import os
import importlib.util

from kafka import KafkaProducer
from kafka.errors import KafkaError

# ============================================================
# KAFKA CONFIGURATION
# ============================================================

KAFKA_SERVER = "localhost:9092"

TOPIC_NAME = "ott_streaming_events"

MESSAGE_DELAY = 1

# ============================================================
# STEP 1: LOAD DATA FROM "Data create.py"
# ============================================================

# Get the exact folder where producer.py is located
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Create the full path to Data create.py
DATA_FILE = os.path.join(
    BASE_DIR,
    "Data create.py"
)


# ============================================================
# CHECK IF DATA CREATE FILE EXISTS
# ============================================================

if not os.path.isfile(DATA_FILE):

    print()
    print("=" * 70)
    print("ERROR: DATA CREATE FILE NOT FOUND")
    print("=" * 70)

    print()
    print("Python is looking for:")
    print(DATA_FILE)

    print()
    print("Make sure your folder contains:")
    print("    Data create.py")
    print("    producer.py")

    print()
    print("Both files must be in the same folder.")

    print("=" * 70)

    exit()


# ============================================================
# IMPORT "Data create.py"
# ============================================================

try:

    spec = importlib.util.spec_from_file_location(
        "data_create",
        DATA_FILE
    )

    data_create = importlib.util.module_from_spec(
        spec
    )

    spec.loader.exec_module(
        data_create
    )

    # Get the dataset creation function
    create_sample_dataset = (
        data_create.create_sample_dataset
    )

except Exception as error:

    print()
    print("=" * 70)
    print("ERROR: COULD NOT LOAD DATA CREATE.PY")
    print("=" * 70)

    print(error)

    print("=" * 70)

    exit()


# ============================================================
# KAFKA CONFIGURATION
# ============================================================

KAFKA_SERVER = "localhost:9092"

TOPIC_NAME = "ott_streaming_events"

# Time between messages
MESSAGE_DELAY = 1


# ============================================================
# CREATE KAFKA PRODUCER
# ============================================================

def create_kafka_producer():

    try:

        producer = KafkaProducer(

            # Kafka server
            bootstrap_servers=[
                KAFKA_SERVER
            ],

            # Convert Python dictionary
            # into JSON bytes
            value_serializer=lambda value:
                json.dumps(value).encode("utf-8"),

            # Wait for Kafka confirmation
            acks="all"
        )

        print()
        print("=" * 70)
        print("CONNECTED TO KAFKA SUCCESSFULLY")
        print("=" * 70)

        print(
            f"Kafka Server : {KAFKA_SERVER}"
        )

        print(
            f"Kafka Topic  : {TOPIC_NAME}"
        )

        print("=" * 70)

        return producer

    except KafkaError as error:

        print()
        print("=" * 70)
        print("ERROR: COULD NOT CONNECT TO KAFKA")
        print("=" * 70)

        print()
        print("Make sure Kafka is running on:")
        print(KAFKA_SERVER)

        print()
        print("Kafka Error:")
        print(error)

        print("=" * 70)

        return None


# ============================================================
# DISPLAY EVENT
# ============================================================

def display_event(
    event,
    metadata,
    message_number,
    total_messages
):

    print()
    print("-" * 70)

    print(
        f"Message      : "
        f"{message_number}/{total_messages}"
    )

    print(
        f"Event ID     : "
        f"{event.get('event_id', 'N/A')}"
    )

    print(
        f"Event Source : "
        f"{event.get('event_source', 'N/A')}"
    )

    print(
        f"Content      : "
        f"{event.get('content_name', 'N/A')}"
    )

    print(
        f"User ID      : "
        f"{event.get('user_id', 'N/A')}"
    )

    print(
        f"Topic        : "
        f"{metadata.topic}"
    )

    print(
        f"Partition    : "
        f"{metadata.partition}"
    )

    print(
        f"Offset       : "
        f"{metadata.offset}"
    )

    print("-" * 70)


# ============================================================
# STREAM DATA TO KAFKA
# ============================================================

def stream_to_kafka(
    producer,
    dataset
):

    total_messages = len(dataset)

    message_number = 0

    print()
    print("=" * 70)
    print("STARTING OTT DATA STREAM")
    print("=" * 70)

    print(
        f"Total Records : "
        f"{total_messages}"
    )

    print(
        f"Kafka Topic   : "
        f"{TOPIC_NAME}"
    )

    print(
        f"Message Delay : "
        f"{MESSAGE_DELAY} second(s)"
    )

    print()
    print("Press CTRL + C to stop the producer.")

    print("=" * 70)


    try:

        # ----------------------------------------------------
        # SEND EVERY RECORD TO KAFKA
        # ----------------------------------------------------

        for event in dataset:

            # Send event to Kafka
            future = producer.send(
                TOPIC_NAME,
                value=event
            )

            # Wait for Kafka acknowledgement
            metadata = future.get(
                timeout=10
            )

            # Increase message counter
            message_number += 1

            # Display information
            display_event(
                event,
                metadata,
                message_number,
                total_messages
            )

            # Wait before sending next message
            time.sleep(
                MESSAGE_DELAY
            )


        # ----------------------------------------------------
        # STREAM COMPLETED
        # ----------------------------------------------------

        print()
        print("=" * 70)
        print("DATA STREAMING COMPLETED")
        print("=" * 70)

        print(
            f"Total Messages Sent: "
            f"{message_number}"
        )

        print("=" * 70)


    except KeyboardInterrupt:

        print()
        print("=" * 70)
        print("PRODUCER STOPPED BY USER")
        print("=" * 70)

        print(
            f"Messages Sent: "
            f"{message_number}"
        )

        print("=" * 70)


    except KafkaError as error:

        print()
        print("=" * 70)
        print("KAFKA ERROR")
        print("=" * 70)

        print(error)

        print("=" * 70)


    except Exception as error:

        print()
        print("=" * 70)
        print("UNEXPECTED ERROR")
        print("=" * 70)

        print(error)

        print("=" * 70)


# ============================================================
# MAIN PROGRAM
# ============================================================

def main():

    print()
    print("=" * 70)
    print("OTT KAFKA STREAMING PRODUCER")
    print("=" * 70)


    # ========================================================
    # STEP 1: GENERATE DATASET
    # ========================================================

    print()
    print("STEP 1: GENERATING SAMPLE DATASET")
    print("-" * 70)

    try:

        # Your Data create.py creates 500 records
        dataset = create_sample_dataset()

        print(
            f"Dataset generated successfully."
        )

        print(
            f"Total records created: "
            f"{len(dataset)}"
        )

    except Exception as error:

        print()
        print("=" * 70)
        print("ERROR WHILE CREATING DATASET")
        print("=" * 70)

        print(error)

        print("=" * 70)

        return


    # ========================================================
    # STEP 2: CONNECT TO KAFKA
    # ========================================================

    print()
    print("STEP 2: CONNECTING TO KAFKA")
    print("-" * 70)

    producer = create_kafka_producer()


    # Stop if connection failed
    if producer is None:

        return


    # ========================================================
    # STEP 3: STREAM DATA
    # ========================================================

    print()
    print("STEP 3: STREAMING DATA TO KAFKA")
    print("-" * 70)


    try:

        stream_to_kafka(
            producer,
            dataset
        )

    finally:

        # Close Kafka connection
        producer.close()

        print()
        print("=" * 70)
        print("KAFKA PRODUCER CLOSED")
        print("=" * 70)


# ============================================================
# PROGRAM START
# ============================================================

if __name__ == "__main__":

    main()