package fr.remyqueny.sncf.producer;

import com.fasterxml.jackson.annotation.JsonProperty;

public record TripUpdateEvent(
        @JsonProperty("event_id") String eventId,
        @JsonProperty("event_type") String eventType,
        @JsonProperty("source") String source,
        @JsonProperty("schema_version") int schemaVersion,
        @JsonProperty("trip_id") String tripId,
        @JsonProperty("route_id") String routeId,
        @JsonProperty("stop_id") String stopId,
        @JsonProperty("scheduled_timestamp") String scheduledTimestamp,
        @JsonProperty("estimated_timestamp") String estimatedTimestamp,
        @JsonProperty("delay_seconds") int delaySeconds,
        @JsonProperty("event_timestamp") String eventTimestamp,
        @JsonProperty("ingested_at") String ingestedAt
) {
}