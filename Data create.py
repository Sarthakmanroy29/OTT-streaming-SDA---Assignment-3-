import random
import json
from datetime import datetime
from faker import Faker

# ============================================================
# INITIALIZATION
# ============================================================

fake = Faker()

# Number of records for EACH data source
RECORDS_PER_SOURCE = 100


# ============================================================
# OTT CONTENT
# ============================================================

CONTENT = [
    ("C001", "The Last Horizon"),
    ("C002", "Midnight Mumbai"),
    ("C003", "Code Red"),
    ("C004", "The Final Season"),
    ("C005", "Delhi Files"),
    ("C006", "Beyond the Stars"),
    ("C007", "Shadow Protocol"),
    ("C008", "The Last Kingdom"),
    ("C009", "City of Dreams"),
    ("C010", "Digital Hearts")
]


# ============================================================
# COMMON VALUES
# ============================================================

DEVICES = [
    "mobile",
    "smart_tv",
    "laptop",
    "tablet"
]

COUNTRIES = [
    "India",
    "USA",
    "UK",
    "Canada",
    "Australia",
    "Singapore"
]

VIEWER_EVENT_TYPES = [
    "play",
    "pause",
    "skip",
    "exit",
    "complete"
]

INTERACTION_TYPES = [
    "like",
    "rating",
    "search",
    "share",
    "comment"
]

AD_EVENT_TYPES = [
    "impression",
    "click",
    "skip",
    "complete"
]

SENTIMENT_TYPES = [
    "positive",
    "neutral",
    "negative"
]

SOCIAL_PLATFORMS = [
    "Instagram",
    "X",
    "YouTube",
    "Facebook"
]

SEARCH_QUERIES = [
    "best thriller series",
    "latest episode",
    "popular shows",
    "new movies",
    "The Last Horizon",
    "Delhi Files",
    "Code Red",
    "trending series"
]


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def get_content():
    """
    Select a random OTT content.
    """
    return random.choice(CONTENT)


def get_user_id():
    """
    Generate a fictional user ID.
    """
    return f"U{random.randint(1000, 9999)}"


def get_session_id():
    """
    Generate a fictional session ID.
    """
    return f"S{random.randint(10000, 99999)}"


def get_timestamp():
    """
    Generate current timestamp.
    """
    return datetime.now().isoformat(timespec="seconds")


def get_event_id(prefix):
    """
    Generate a unique-style event ID.
    """
    return f"{prefix}{random.randint(100000, 999999)}"


# ============================================================
# SOURCE 1
# OTT VIEWER EVENTS
# ============================================================

def generate_viewer_event():

    content_id, content_name = get_content()

    event = {
        "event_id": get_event_id("VE"),
        "timestamp": get_timestamp(),
        "event_source": "viewer_events",
        "user_id": get_user_id(),
        "content_id": content_id,
        "content_name": content_name,
        "event_type": random.choice(VIEWER_EVENT_TYPES),
        "watch_time_seconds": random.randint(5, 3600),
        "device_type": random.choice(DEVICES),
        "country": random.choice(COUNTRIES),
        "session_id": get_session_id()
    }

    return event


# ============================================================
# SOURCE 2
# VIDEO STREAMING QoS
# ============================================================

def generate_qos_event():

    content_id, content_name = get_content()

    event = {
        "event_id": get_event_id("QE"),
        "timestamp": get_timestamp(),
        "event_source": "streaming_qos",
        "session_id": get_session_id(),
        "user_id": get_user_id(),
        "content_id": content_id,
        "content_name": content_name,
        "buffering_seconds": round(
            random.uniform(0, 30),
            2
        ),
        "bitrate_kbps": random.randint(
            500,
            10000
        ),
        "latency_ms": random.randint(
            20,
            500
        ),
        "playback_error": random.choice([
            True,
            False
        ]),
        "device_type": random.choice(DEVICES)
    }

    return event


# ============================================================
# SOURCE 3
# CONTENT INTERACTION EVENTS
# ============================================================

def generate_interaction_event():

    content_id, content_name = get_content()

    interaction_type = random.choice(
        INTERACTION_TYPES
    )

    rating = None
    search_query = None

    # Generate rating only for rating interaction
    if interaction_type == "rating":
        rating = random.randint(1, 5)

    # Generate search query only for search interaction
    if interaction_type == "search":
        search_query = random.choice(
            SEARCH_QUERIES
        )

    event = {
        "event_id": get_event_id("IE"),
        "timestamp": get_timestamp(),
        "event_source": "content_interactions",
        "user_id": get_user_id(),
        "content_id": content_id,
        "content_name": content_name,
        "interaction_type": interaction_type,
        "rating": rating,
        "search_query": search_query,
        "device_type": random.choice(DEVICES)
    }

    return event


# ============================================================
# SOURCE 4
# ADVERTISEMENT EVENTS
# ============================================================

def generate_ad_event():

    content_id, content_name = get_content()

    ad_duration = random.choice([
        15,
        20,
        30,
        45,
        60
    ])

    ad_event_type = random.choice(
        AD_EVENT_TYPES
    )

    # Generate watched time based on ad event
    if ad_event_type == "complete":

        watched_seconds = ad_duration

    elif ad_event_type == "impression":

        watched_seconds = random.randint(
            0,
            ad_duration
        )

    elif ad_event_type == "skip":

        watched_seconds = random.randint(
            1,
            max(1, ad_duration - 1)
        )

    else:  # click

        watched_seconds = random.randint(
            1,
            ad_duration
        )

    # Calculate completion percentage
    completion_rate = round(
        (watched_seconds / ad_duration) * 100,
        2
    )

    # Make sure it cannot exceed 100
    completion_rate = min(
        completion_rate,
        100
    )

    event = {
        "event_id": get_event_id("AE"),
        "timestamp": get_timestamp(),
        "event_source": "ad_events",
        "user_id": get_user_id(),
        "content_id": content_id,
        "content_name": content_name,
        "ad_id": f"AD{random.randint(1000, 9999)}",
        "ad_event_type": ad_event_type,
        "ad_duration_seconds": ad_duration,
        "watched_seconds": watched_seconds,
        "completion_rate": completion_rate
    }

    return event


# ============================================================
# SOURCE 5
# SOCIAL MEDIA SENTIMENT
# ============================================================

def generate_sentiment_event():

    content_id, content_name = get_content()

    sentiment = random.choice(
        SENTIMENT_TYPES
    )

    # -------------------------
    # POSITIVE
    # -------------------------

    if sentiment == "positive":

        sentiment_score = round(
            random.uniform(0.1, 1.0),
            2
        )

        post_text = random.choice([
            "Amazing episode!",
            "Loved this show!",
            "What a great series!",
            "The latest episode was fantastic!",
            "This show is really good!"
        ])

    # -------------------------
    # NEGATIVE
    # -------------------------

    elif sentiment == "negative":

        sentiment_score = round(
            random.uniform(-1.0, -0.1),
            2
        )

        post_text = random.choice([
            "The episode was disappointing.",
            "Not happy with the latest episode.",
            "The story could have been better.",
            "The episode was too slow.",
            "I did not enjoy this episode."
        ])

    # -------------------------
    # NEUTRAL
    # -------------------------

    else:

        sentiment_score = round(
            random.uniform(-0.1, 0.1),
            2
        )

        post_text = random.choice([
            "The episode was okay.",
            "It was an average episode.",
            "Not bad, not great.",
            "The show was interesting.",
            "The episode was fine."
        ])

    event = {
        "event_id": get_event_id("SE"),
        "timestamp": get_timestamp(),
        "event_source": "social_sentiment",
        "platform": random.choice(
            SOCIAL_PLATFORMS
        ),
        "user_id": get_user_id(),
        "content_id": content_id,
        "content_name": content_name,
        "post_text": post_text,
        "sentiment": sentiment,
        "sentiment_score": sentiment_score
    }

    return event


# ============================================================
# CREATE 500 RECORD DATASET
# ============================================================

def create_sample_dataset():

    dataset = []

    # -----------------------------------------
    # 100 VIEWER EVENTS
    # -----------------------------------------

    for _ in range(RECORDS_PER_SOURCE):

        dataset.append(
            generate_viewer_event()
        )

    # -----------------------------------------
    # 100 QoS EVENTS
    # -----------------------------------------

    for _ in range(RECORDS_PER_SOURCE):

        dataset.append(
            generate_qos_event()
        )

    # -----------------------------------------
    # 100 CONTENT INTERACTION EVENTS
    # -----------------------------------------

    for _ in range(RECORDS_PER_SOURCE):

        dataset.append(
            generate_interaction_event()
        )

    # -----------------------------------------
    # 100 ADVERTISEMENT EVENTS
    # -----------------------------------------

    for _ in range(RECORDS_PER_SOURCE):

        dataset.append(
            generate_ad_event()
        )

    # -----------------------------------------
    # 100 SOCIAL SENTIMENT EVENTS
    # -----------------------------------------

    for _ in range(RECORDS_PER_SOURCE):

        dataset.append(
            generate_sentiment_event()
        )

    return dataset


# ============================================================
# COUNT EVENTS BY SOURCE
# ============================================================

def count_events_by_source(dataset):

    counts = {
        "viewer_events": 0,
        "streaming_qos": 0,
        "content_interactions": 0,
        "ad_events": 0,
        "social_sentiment": 0
    }

    for event in dataset:

        source = event["event_source"]

        if source in counts:
            counts[source] += 1

    return counts


# ============================================================
# MAIN PROGRAM
# ============================================================

if __name__ == "__main__":

    print("\n")
    print("=" * 70)
    print("OTT STREAMING SAMPLE DATASET GENERATOR")
    print("=" * 70)

    # Create exactly 500 records
    dataset = create_sample_dataset()

    # Count records
    counts = count_events_by_source(dataset)

    # -----------------------------------------
    # DISPLAY SUMMARY
    # -----------------------------------------

    print("\nDATASET GENERATION COMPLETED")
    print("-" * 70)

    print(
        f"Viewer Events             : {counts['viewer_events']}"
    )

    print(
        f"Streaming QoS Events      : {counts['streaming_qos']}"
    )

    print(
        f"Content Interaction Events: {counts['content_interactions']}"
    )

    print(
        f"Advertisement Events      : {counts['ad_events']}"
    )

    print(
        f"Social Sentiment Events   : {counts['social_sentiment']}"
    )

    print("-" * 70)

    print(
        f"TOTAL RECORDS             : {len(dataset)}"
    )

    print("=" * 70)

    # -----------------------------------------
    # VERIFY TOTAL
    # -----------------------------------------

    if len(dataset) == 500:

        print("SUCCESS: Exactly 500 records created.")

    else:

        print(
            f"ERROR: Expected 500 records but got {len(dataset)}"
        )

    print("=" * 70)

    # -----------------------------------------
    # DISPLAY FIRST 5 RECORDS
    # -----------------------------------------

    print("\nFIRST 5 SAMPLE RECORDS")
    print("=" * 70)

    for i, event in enumerate(
        dataset[:5],
        start=1
    ):

        print(f"\nRecord {i}")
        print("-" * 70)

        print(
            json.dumps(
                event,
                indent=4
            )
        )

    print("\n")
    print("=" * 70)
    print("DATASET READY FOR KAFKA PRODUCER")
    print("=" * 70)