package fr.remyqueny.sncf.producer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class TripUpdateEventTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void shouldSerializeUsingSnakeCaseContractFields() throws Exception {
        TripUpdateEvent event = new TripUpdateEvent(
                "event-123",
                "trip_update",
                "simulation_java",
                1,
                "TRAIN-H-001",
                "H",
                "stop_87271007",
                "2026-09-21T14:30:00Z",
                "2026-09-21T14:38:00Z",
                480,
                "2026-09-21T14:25:00Z",
                "2026-09-21T14:25:02Z"
        );

        String json = objectMapper.writeValueAsString(event);
        JsonNode payload = objectMapper.readTree(json);

        assertEquals("TRAIN-H-001", payload.get("trip_id").asText());
        assertEquals(480, payload.get("delay_seconds").asInt());

        assertFalse(payload.has("tripId"));
        assertFalse(payload.has("delaySeconds"));
    }

    @Test
    void shouldCreateAConsistentSimulatedDelay() {
        TripUpdateEvent event = TripUpdateEventFactory.createSimulatedEvent();

        Instant scheduledTimestamp = Instant.parse(event.scheduledTimestamp());
        Instant estimatedTimestamp = Instant.parse(event.estimatedTimestamp());

        long computedDelaySeconds = Duration.between(
                scheduledTimestamp,
                estimatedTimestamp
        ).getSeconds();

        assertEquals("trip_update", event.eventType());
        assertEquals("simulation_java", event.source());
        assertEquals(1, event.schemaVersion());
        assertEquals(event.delaySeconds(), computedDelaySeconds);
    }
}