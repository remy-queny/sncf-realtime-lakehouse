package fr.remyqueny.sncf.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.transit.realtime.GtfsRealtime.TripDescriptor;
import com.google.transit.realtime.GtfsRealtime.TripUpdate;
import com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeEvent;
import com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GtfsStopEventMapperTest {

    private static final String ROUTE_ID = "ROUTE-TEST-001";

    private static final Instant OBSERVATION =
            Instant.parse("2026-10-08T17:18:01Z");

    private static final Instant INGESTED_AT =
            Instant.parse("2026-10-08T17:20:00Z");

    private static final Instant ESTIMATED =
            Instant.parse("2026-10-08T17:30:00Z");

    @Test
    void shouldPreferArrivalAndSerializeCanonicalFields() throws Exception {
        StopTimeUpdate stop = stopBuilder()
                .setArrival(timedEvent(600))
                .setDeparture(timedEvent(1200))
                .build();

        TripUpdateEvent event = map(stop);

        assertEquals("trip_update", event.eventType());
        assertEquals("sncf_gtfs_rt", event.source());
        assertEquals(1, event.schemaVersion());
        assertEquals("TRIP-TEST-001", event.tripId());
        assertEquals(ROUTE_ID, event.routeId());
        assertEquals("STOP-TEST-001", event.stopId());
        assertEquals(600, event.delaySeconds());

        assertEquals(
                ESTIMATED.minusSeconds(600).toString(),
                event.scheduledTimestamp()
        );
        assertEquals(ESTIMATED.toString(), event.estimatedTimestamp());
        assertEquals(OBSERVATION.toString(), event.eventTimestamp());
        assertEquals(INGESTED_AT.toString(), event.ingestedAt());
        assertEquals(3, UUID.fromString(event.eventId()).version());

        ObjectMapper objectMapper = new ObjectMapper();
        var payload = objectMapper.readTree(
                objectMapper.writeValueAsString(event)
        );

        assertEquals("sncf_gtfs_rt", payload.get("source").asText());
        assertEquals(600, payload.get("delay_seconds").asInt());
        assertEquals(ROUTE_ID, payload.get("route_id").asText());
        assertEquals(
                OBSERVATION.toString(),
                payload.get("event_timestamp").asText()
        );
    }

    @Test
    void shouldFallbackToDepartureAndKeepExplicitZeroDelay() {
        StopTimeEvent incompleteArrival = StopTimeEvent.newBuilder()
                .setTime(ESTIMATED.getEpochSecond())
                .build();

        StopTimeUpdate stop = stopBuilder()
                .setArrival(incompleteArrival)
                .setDeparture(timedEvent(0))
                .build();

        TripUpdateEvent event = map(stop);

        assertEquals(0, event.delaySeconds());
        assertEquals(ESTIMATED.toString(), event.scheduledTimestamp());
        assertEquals(ESTIMATED.toString(), event.estimatedTimestamp());
    }

    @Test
    void shouldRejectEventsWithoutExplicitDelay() {
        StopTimeEvent withoutDelay = StopTimeEvent.newBuilder()
                .setTime(ESTIMATED.getEpochSecond())
                .build();

        StopTimeUpdate stop = stopBuilder()
                .setArrival(withoutDelay)
                .setDeparture(withoutDelay)
                .build();

        assertThrows(
                IllegalArgumentException.class,
                () -> map(stop)
        );
    }

    @Test
    void shouldPreserveNegativeDelay() {
        TripUpdateEvent event = map(
                stopBuilder().setArrival(timedEvent(-120)).build()
        );

        assertEquals(-120, event.delaySeconds());
        assertEquals(
                ESTIMATED.plusSeconds(120).toString(),
                event.scheduledTimestamp()
        );
    }

    @Test
    void shouldKeepEventIdWhenOnlyIngestionTimeChanges() {
        StopTimeUpdate stop = stopBuilder()
                .setArrival(timedEvent(600))
                .build();

        TripUpdateEvent first = map(stop);

        TripUpdateEvent second = GtfsStopEventMapper.map(
                scheduledTrip(),
                stop,
                ROUTE_ID,
                OBSERVATION,
                INGESTED_AT.plusSeconds(60)
        );

        assertEquals(first.eventId(), second.eventId());
        assertNotEquals(first.ingestedAt(), second.ingestedAt());
    }

    @Test
    void shouldChangeEventIdForAnotherSnapshot() {
        StopTimeUpdate stop = stopBuilder()
                .setArrival(timedEvent(600))
                .build();

        TripUpdateEvent first = map(stop);

        TripUpdateEvent second = GtfsStopEventMapper.map(
                scheduledTrip(),
                stop,
                ROUTE_ID,
                OBSERVATION.plusSeconds(120),
                INGESTED_AT
        );

        assertNotEquals(first.eventId(), second.eventId());
    }

    @Test
    void shouldRejectUnsupportedTripsAndSkippedStops() {
        StopTimeUpdate stop = stopBuilder()
                .setArrival(timedEvent(600))
                .build();

        var excludedRelationships =
                new TripDescriptor.ScheduleRelationship[]{
                        TripDescriptor.ScheduleRelationship.ADDED,
                        TripDescriptor.ScheduleRelationship.CANCELED
                };

        for (var relationship : excludedRelationships) {
            TripUpdate excludedTrip = scheduledTrip().toBuilder()
                    .setTrip(
                            scheduledTrip().getTrip().toBuilder()
                                    .setScheduleRelationship(relationship)
                    )
                    .build();

            assertThrows(
                    IllegalArgumentException.class,
                    () -> GtfsStopEventMapper.map(
                            excludedTrip,
                            stop,
                            ROUTE_ID,
                            OBSERVATION,
                            INGESTED_AT
                    )
            );
        }

        StopTimeUpdate skippedStop = stop.toBuilder()
                .setScheduleRelationship(
                        StopTimeUpdate.ScheduleRelationship.SKIPPED
                )
                .build();

        assertThrows(
                IllegalArgumentException.class,
                () -> map(skippedStop)
        );
    }

    @Test
    void shouldRejectMissingRouteId() {
        StopTimeUpdate stop = stopBuilder()
                .setArrival(timedEvent(600))
                .build();

        assertThrows(
                IllegalArgumentException.class,
                () -> GtfsStopEventMapper.map(
                        scheduledTrip(),
                        stop,
                        null,
                        OBSERVATION,
                        INGESTED_AT
                )
        );
    }

    private static TripUpdate scheduledTrip() {
        return TripUpdate.newBuilder()
                .setTrip(
                        TripDescriptor.newBuilder()
                                .setTripId("TRIP-TEST-001")
                                .setStartDate("20261008")
                                .setStartTime("17:00:00")
                )
                .build();
    }

    private static StopTimeUpdate.Builder stopBuilder() {
        return StopTimeUpdate.newBuilder()
                .setStopId("STOP-TEST-001")
                .setStopSequence(1);
    }

    private static StopTimeEvent timedEvent(int delay) {
        return StopTimeEvent.newBuilder()
                .setTime(ESTIMATED.getEpochSecond())
                .setDelay(delay)
                .build();
    }

    private static TripUpdateEvent map(StopTimeUpdate stop) {
        return GtfsStopEventMapper.map(
                scheduledTrip(),
                stop,
                ROUTE_ID,
                OBSERVATION,
                INGESTED_AT
        );
    }
}