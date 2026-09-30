# OTT-streaming-SDA---Assignment-3-
OTT Streaming Analytics
Industry
Media & Entertainment – OTT/Video Streaming

**Project Description**
This project demonstrates a real-time OTT streaming analytics pipeline using Apache Kafka and Python.

The system generates synthetic streaming events from five data sources:

OTT Viewer Events
Video Streaming QoS Logs
Content Interaction Events
Advertisement Events
Social Media Sentiment
**Dataset**
The Python dataset generator creates 500 synthetic records:

100 Viewer Events
100 Streaming QoS Events
100 Content Interaction Events
100 Advertisement Events
100 Social Media Sentiment Events

**Data Pipeline Architecture**
OTT Viewer Events
Streaming QoS
Content Interactions
Advertisement Events
Social Media Sentiment
            |
            v
     Python Data Generator
            |
            v
       Kafka Producer
            |
            v
 Kafka Topic: ott_streaming_events
            |
            v
       Kafka Consumer
            |
            v
    Streaming Analytics
            |
            v
          MySQL
            |
            v
         Grafana
            |
            v
   OTT Analytics Dashboard

   **Kafka Configuration**
Kafka Topic
ott_streaming_events
Kafka Server
localhost:9092
Consumer Group
ott-streaming-consumer-group

The Kafka consumer reads JSON messages from the ott_streaming_events topic and performs basic streaming analytics.

**Consumer**

The consumer.py program connects to Kafka and consumes messages from the ott_streaming_events topic.

For each event, the consumer identifies the event source and performs source-specific analytics.

The consumer tracks:

Total messages consumed
Events by source
Viewer event types
Top content
Total watch time
Total buffering time
Total playback errors
Average buffering
Average latency
Social media sentiment

The consumer also displays individual streaming events in the terminal along with Kafka topic, partition, and offset information.

**MySQL Database**

The processed/sample OTT data is stored in MySQL.

Database
sample_data
Table
ott_streaming_events

The table contains fields including:

event_id
event_timestamp
event_source
user_id
content_id
content_name
device_type
bitrate_kbps
buffering_seconds
completion_rate
latency_ms
playback_error
sentiment
sentiment_score
session_id
watch_time_seconds
watched_seconds

**Grafana Dashboard**

The Grafana dashboard is designed to provide an overview of OTT streaming performance and audience behaviour.

The dashboard contains visualizations such as:

Total Streaming Events
Events by Source
Streaming Events Over Time
Average Streaming Latency
Average Buffering Time
Playback Errors
Top 10 Content
Average Watch Time by Content
Social Media Sentiment Distribution
Advertisement Completion Rate

The exported Grafana dashboard configuration is available in:

dashboard/ott_streaming_dashboard.json

Business Decision Enabled

The streaming pipeline supports the decision of whether an OTT platform should immediately change its recommendation or homepage strategy when a show suddenly gains viewer engagement.

The streaming pipeline continuously analyzes watch time, searches, skips, completion rates, likes, shares, and social-media sentiment. If these signals indicate unusual traction for a particular show or episode, the platform manager can increase its visibility on the homepage or push it to relevant users.

Real-time analytics allows the OTT platform to respond to emerging viewer interest while it is happening rather than waiting for a daily batch report.

