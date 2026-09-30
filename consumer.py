import json
import time
from collections import Counter

from kafka import KafkaConsumer
from kafka.errors import KafkaError

# ============================================================
# KAFKA CONFIGURATION
# ============================================================

KAFKA_SERVER = "localhost:9092"
TOPIC_NAME = "ott_streaming_events"

# Consumer group name
GROUP_ID = "ott-streaming-consumer-group"

# ============================================================
# CREATE KAFKA CONSUMER
# ============================================================

def create_kafka_consumer():
    try:
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=[KAFKA_SERVER],
            group_id=GROUP_ID,

            # Start from the beginning if this group has no
            # previously committed offset
            auto_offset_reset="earliest",

            # Convert JSON bytes back into a Python dictionary
            value_deserializer=lambda value: json.loads(
                value.decode("utf-8")
            ),

            enable_auto_commit=True,

            # Avoid waiting too long when polling
            consumer_timeout_ms=1000
        )

        print()
        print("=" * 70)
        print("CONNECTED TO KAFKA SUCCESSFULLY")
        print("=" * 70)
        print(f"Kafka Server : {KAFKA_SERVER}")
        print(f"Kafka Topic  : {TOPIC_NAME}")
        print(f"Consumer Group: {GROUP_ID}")
        print("=" * 70)

        return consumer

    except KafkaError as error:
        print()
        print("=" * 70)
        print("ERROR: COULD NOT CONNECT TO KAFKA")
        print("=" * 70)
        print(error)
        print()
        print("Make sure Kafka is running on localhost:9092")
        print("=" * 70)
        return None


# ============================================================
# DISPLAY EVENT
# ============================================================

def display_event(event, message_number, metadata):
    print()
    print("-" * 70)
    print(f"Message Number : {message_number}")
    print(f"Event ID       : {event.get('event_id', 'N/A')}")
    print(f"Timestamp      : {event.get('timestamp', 'N/A')}")
    print(f"Event Source   : {event.get('event_source', 'N/A')}")
    print(f"User ID        : {event.get('user_id', 'N/A')}")
    print(f"Content        : {event.get('content_name', 'N/A')}")
    print(f"Topic          : {metadata.topic}")
    print(f"Partition      : {metadata.partition}")
    print(f"Offset         : {metadata.offset}")

    # Print source-specific information
    source = event.get("event_source")

    if source == "viewer_events":
        print(f"Event Type     : {event.get('event_type', 'N/A')}")
        print(f"Watch Time     : {event.get('watch_time_seconds', 0)} sec")
        print(f"Device         : {event.get('device_type', 'N/A')}")
        print(f"Country        : {event.get('country', 'N/A')}")
        print(f"Session ID     : {event.get('session_id', 'N/A')}")

    elif source == "streaming_qos":
        print(f"Buffering      : {event.get('buffering_seconds', 0)} sec")
        print(f"Bitrate        : {event.get('bitrate_kbps', 0)} kbps")
        print(f"Latency        : {event.get('latency_ms', 0)} ms")
        print(f"Playback Error : {event.get('playback_error', False)}")
        print(f"Device         : {event.get('device_type', 'N/A')}")

    elif source == "content_interactions":
        print(f"Interaction    : {event.get('interaction_type', 'N/A')}")
        print(f"Rating         : {event.get('rating', 'N/A')}")
        print(f"Search Query   : {event.get('search_query', 'N/A')}")
        print(f"Device         : {event.get('device_type', 'N/A')}")

    elif source == "ad_events":
        print(f"Ad ID          : {event.get('ad_id', 'N/A')}")
        print(f"Ad Event       : {event.get('ad_event_type', 'N/A')}")
        print(f"Ad Duration    : {event.get('ad_duration_seconds', 0)} sec")
        print(f"Watched        : {event.get('watched_seconds', 0)} sec")
        print(f"Completion     : {event.get('completion_rate', 0)}%")

    elif source == "social_sentiment":
        print(f"Platform       : {event.get('platform', 'N/A')}")
        print(f"Sentiment      : {event.get('sentiment', 'N/A')}")
        print(f"Score          : {event.get('sentiment_score', 0)}")
        print(f"Post           : {event.get('post_text', 'N/A')}")

    print("-" * 70)


# ============================================================
# STREAM CONSUMER
# ============================================================

def consume_stream(consumer):
    message_count = 0

    source_counter = Counter()
    event_type_counter = Counter()
    content_counter = Counter()

    total_watch_time = 0
    total_buffering = 0
    total_latency = 0
    playback_errors = 0
    sentiment_counter = Counter()

    print()
    print("=" * 70)
    print("STARTING OTT STREAMING CONSUMER")
    print("=" * 70)
    print("Waiting for messages...")
    print("Press CTRL + C to stop.")
    print("=" * 70)

    try:
        while True:
            records_received = False

            # Poll Kafka for a small batch of records
            records = consumer.poll(timeout_ms=1000)

            for topic_partition, messages in records.items():
                for message in messages:
                    records_received = True
                    event = message.value
                    message_count += 1

                    # --------------------------------------------
                    # BASIC STREAM ANALYTICS
                    # --------------------------------------------

                    source = event.get("event_source", "unknown")
                    content = event.get("content_name", "unknown")

                    source_counter[source] += 1
                    content_counter[content] += 1

                    # Viewer analytics
                    if source == "viewer_events":
                        event_type_counter[
                            event.get("event_type", "unknown")
                        ] += 1

                        total_watch_time += event.get(
                            "watch_time_seconds", 0
                        )

                    # QoS analytics
                    elif source == "streaming_qos":
                        total_buffering += event.get(
                            "buffering_seconds", 0
                        )

                        total_latency += event.get(
                            "latency_ms", 0
                        )

                        if event.get("playback_error", False):
                            playback_errors += 1

                    # Social sentiment analytics
                    elif source == "social_sentiment":
                        sentiment_counter[
                            event.get("sentiment", "unknown")
                        ] += 1

                    # Display event
                    display_event(
                        event,
                        message_count,
                        message
                    )

            # If no records arrived during this poll, continue waiting
            if not records_received:
                time.sleep(0.2)

    except KeyboardInterrupt:
        print()
        print("=" * 70)
        print("CONSUMER STOPPED BY USER")
        print("=" * 70)

    except KafkaError as error:
        print()
        print("=" * 70)
        print("KAFKA ERROR")
        print("=" * 70)
        print(error)
        print("=" * 70)

    finally:
        print_summary(
            message_count,
            source_counter,
            event_type_counter,
            content_counter,
            total_watch_time,
            total_buffering,
            total_latency,
            playback_errors,
            sentiment_counter
        )


# ============================================================
# DISPLAY STREAMING ANALYTICS SUMMARY
# ============================================================

def print_summary(
    message_count,
    source_counter,
    event_type_counter,
    content_counter,
    total_watch_time,
    total_buffering,
    total_latency,
    playback_errors,
    sentiment_counter
):
    print()
    print("=" * 70)
    print("OTT STREAMING ANALYTICS SUMMARY")
    print("=" * 70)

    print(f"Total Messages Consumed : {message_count}")

    print()
    print("EVENTS BY SOURCE")
    print("-" * 70)
    for source, count in source_counter.items():
        print(f"{source:<30}: {count}")

    print()
    print("VIEWER EVENT TYPES")
    print("-" * 70)
    for event_type, count in event_type_counter.items():
        print(f"{event_type:<30}: {count}")

    print()
    print("TOP CONTENT")
    print("-" * 70)
    for content, count in content_counter.most_common(10):
        print(f"{content:<30}: {count}")

    print()
    print("QUALITY OF SERVICE")
    print("-" * 70)
    print(f"Total Watch Time           : {total_watch_time} seconds")
    print(f"Total Buffering Time       : {total_buffering:.2f} seconds")
    print(f"Total Playback Errors      : {playback_errors}")

    qos_count = source_counter.get("streaming_qos", 0)

    if qos_count > 0:
        avg_buffering = total_buffering / qos_count
        avg_latency = total_latency / qos_count

        print(f"Average Buffering          : {avg_buffering:.2f} seconds")
        print(f"Average Latency            : {avg_latency:.2f} ms")

    print()
    print("SOCIAL SENTIMENT")
    print("-" * 70)
    for sentiment, count in sentiment_counter.items():
        print(f"{sentiment:<30}: {count}")

    print("=" * 70)
    print("CONSUMER CLOSED")
    print("=" * 70)


# ============================================================
# MAIN PROGRAM
# ============================================================

def main():
    print()
    print("=" * 70)
    print("OTT KAFKA STREAMING CONSUMER")
    print("=" * 70)

    consumer = create_kafka_consumer()

    if consumer is None:
        return

    try:
        consume_stream(consumer)

    finally:
        consumer.close()
        print()
        print("Kafka consumer connection closed.")


# ============================================================
# PROGRAM START
# ============================================================

if __name__ == "__main__":
    main()
