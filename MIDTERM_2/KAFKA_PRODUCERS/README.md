# Telemetry Kafka Producers

This folder contains simple Kafka producers that generate telemetry events with a realistic mix of:
- stable sources (always present)
- dynamic sources (churn over time)
- drift periods (short windows where a subset of sources changes behavior)

All producers emit the same JSON schema to the same Kafka topic.

## Event schema

```json
{
  "source_id": "string",
  "metric": "cpu_usage | memory_usage | request_latency",
  "value": 0.0,
  "event_time": "2026-02-10T12:34:56.789Z"
}
