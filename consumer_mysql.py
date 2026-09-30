import pandas as pd
import mysql.connector
from mysql.connector import Error


# ============================================================
# CONFIGURATION
# ============================================================

CSV_FILE = r"C:\Users\Mohit\OneDrive - fsm.ac.in\Desktop\FORE Class\Year 2\Sem 4\SDA\Project\sample_data.csv"

MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 3306
MYSQL_DATABASE = "sample_data"
MYSQL_USER = "root"

# CHANGE THIS
MYSQL_PASSWORD = "root"

TABLE_NAME = "ott_streaming_events"


# ============================================================
# READ CSV
# ============================================================

print("=" * 70)
print("OTT STREAMING CSV -> MYSQL")
print("=" * 70)

print("\nReading CSV...")

df = pd.read_csv(CSV_FILE)

print(f"CSV records found: {len(df)}")

print("\nColumns found:")
print(list(df.columns))


# ============================================================
# CLEAN EMPTY VALUES
# ============================================================

df = df.where(pd.notnull(df), None)


# ============================================================
# MYSQL CONNECTION
# ============================================================

try:

    connection = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        database=MYSQL_DATABASE,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD
    )

    print("\nConnected to MySQL successfully.")

except Error as error:

    print("\nMYSQL CONNECTION ERROR")
    print(error)
    exit()


# ============================================================
# CREATE TABLE
# ============================================================

cursor = connection.cursor()

create_table_query = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (

    id BIGINT AUTO_INCREMENT PRIMARY KEY,

    event_id VARCHAR(50) NOT NULL UNIQUE,

    event_timestamp DATETIME,

    event_source VARCHAR(50),

    user_id VARCHAR(50),

    content_id VARCHAR(50),

    content_name VARCHAR(255),

    ad_duration_seconds INT,

    ad_event_type VARCHAR(50),

    ad_id VARCHAR(50),

    bitrate_kbps INT,

    buffering_seconds DECIMAL(10,2),

    completion_rate DECIMAL(10,2),

    country VARCHAR(100),

    device_type VARCHAR(50),

    event_type VARCHAR(50),

    interaction_type VARCHAR(50),

    latency_ms INT,

    platform VARCHAR(50),

    playback_error BOOLEAN,

    post_text TEXT,

    rating INT,

    search_query TEXT,

    sentiment VARCHAR(30),

    sentiment_score DECIMAL(5,2),

    session_id VARCHAR(50),

    watch_time_seconds INT,

    watched_seconds INT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP

)
"""

try:

    cursor.execute(create_table_query)

    connection.commit()

    print("Table created/verified:")
    print(TABLE_NAME)

except Error as error:

    print("TABLE CREATION ERROR")
    print(error)

    cursor.close()
    connection.close()

    exit()


# ============================================================
# INSERT QUERY
# ============================================================

insert_query = f"""
INSERT IGNORE INTO {TABLE_NAME} (

    event_id,
    event_timestamp,
    event_source,
    user_id,
    content_id,
    content_name,

    ad_duration_seconds,
    ad_event_type,
    ad_id,

    bitrate_kbps,
    buffering_seconds,
    completion_rate,

    country,
    device_type,

    event_type,
    interaction_type,

    latency_ms,

    platform,
    playback_error,
    post_text,

    rating,
    search_query,

    sentiment,
    sentiment_score,

    session_id,

    watch_time_seconds,
    watched_seconds

)

VALUES (

    %s, %s, %s, %s, %s, %s,

    %s, %s, %s,

    %s, %s, %s,

    %s, %s,

    %s, %s,

    %s,

    %s, %s, %s,

    %s, %s,

    %s, %s,

    %s,

    %s, %s

)
"""


# ============================================================
# INSERT RECORDS
# ============================================================

print("\nStarting CSV import...")
print("-" * 70)

inserted = 0
skipped = 0


for _, row in df.iterrows():

    try:

        # ----------------------------------------------------
        # Convert timestamp
        # ----------------------------------------------------

        timestamp = pd.to_datetime(
            row["timestamp"]
        )

        timestamp = timestamp.to_pydatetime()


        # ----------------------------------------------------
        # Convert playback error
        # ----------------------------------------------------

        playback_error = row["playback_error"]

        if pd.isna(playback_error):

            playback_error = None

        elif str(playback_error).lower() == "true":

            playback_error = True

        elif str(playback_error).lower() == "false":

            playback_error = False

        else:

            playback_error = None


        # ----------------------------------------------------
        # Prepare values
        # ----------------------------------------------------

        values = (

            row["event_id"],

            timestamp,

            row["event_source"],

            row["user_id"],

            row["content_id"],

            row["content_name"],


            row["ad_duration_seconds"],

            row["ad_event_type"],

            row["ad_id"],


            row["bitrate_kbps"],

            row["buffering_seconds"],

            row["completion_rate"],


            row["country"],

            row["device_type"],


            row["event_type"],

            row["interaction_type"],


            row["latency_ms"],


            row["platform"],

            playback_error,

            row["post_text"],


            row["rating"],

            row["search_query"],


            row["sentiment"],

            row["sentiment_score"],


            row["session_id"],


            row["watch_time_seconds"],

            row["watched_seconds"]

        )


        # ----------------------------------------------------
        # Replace NaN with None
        # ----------------------------------------------------

        values = tuple(

            None if pd.isna(value) else value

            for value in values

        )


        cursor.execute(

            insert_query,

            values

        )

        inserted += 1


        if inserted % 50 == 0:

            connection.commit()

            print(
                f"Records inserted: {inserted}"
            )


    except Exception as error:

        skipped += 1

        print(
            f"Error inserting row: {error}"
        )


# ============================================================
# COMMIT
# ============================================================

connection.commit()


# ============================================================
# VERIFY
# ============================================================

cursor.execute(
    f"SELECT COUNT(*) FROM {TABLE_NAME}"
)

total_records = cursor.fetchone()[0]


print()
print("=" * 70)
print("IMPORT COMPLETED")
print("=" * 70)

print(f"CSV records       : {len(df)}")
print(f"Records inserted   : {inserted}")
print(f"Records skipped    : {skipped}")
print(f"MySQL total        : {total_records}")

print("=" * 70)


# ============================================================
# EVENT SOURCE SUMMARY
# ============================================================

print("\nEVENT SOURCE SUMMARY")
print("-" * 70)

cursor.execute(
    f"""
    SELECT
        event_source,
        COUNT(*) AS total
    FROM {TABLE_NAME}
    GROUP BY event_source
    ORDER BY event_source
    """
)

results = cursor.fetchall()

for source, count in results:

    print(
        f"{source:<30} {count}"
    )


# ============================================================
# CLOSE
# ============================================================

cursor.close()

connection.close()

print()
print("MySQL connection closed.")
print("Done.")