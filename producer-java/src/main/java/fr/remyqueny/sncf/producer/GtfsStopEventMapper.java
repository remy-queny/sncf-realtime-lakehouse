package fr.remyqueny.sncf.producer;

import com.google.transit.realtime.GtfsRealtime.TripDescriptor;
import com.google.transit.realtime.GtfsRealtime.TripUpdate;
import com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeEvent;
import com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

public final class GtfsStopEventMapper {

    private static final String SOURCE = "sncf_gtfs_rt";

    private GtfsStopEventMapper() {
    }

    public static TripUpdateEvent map(
            TripUpdate update,
            StopTimeUpdate stop,
            String routeId,
            Instant observationTimestamp,
            Instant ingestedAt
    ) {
        var trip = update.getTrip();

        if (trip.getScheduleRelationship()
                != TripDescriptor.ScheduleRelationship.SCHEDULED) {
            throw new IllegalArgumentException("Course non planifiée");
        }

        if (stop.getScheduleRelationship()
                != StopTimeUpdate.ScheduleRelationship.SCHEDULED) {
            throw new IllegalArgumentException("Arrêt non planifié");
        }

        if (trip.getTripId().isBlank()
                || stop.getStopId().isBlank()
                || routeId == null
                || routeId.isBlank()) {
            throw new IllegalArgumentException(
                    "Identifiant de course, d'arrêt ou de ligne absent"
            );
        }

        if (observationTimestamp == null
                || observationTimestamp.getEpochSecond() <= 0
                || ingestedAt == null) {
            throw new IllegalArgumentException(
                    "Timestamp d'observation ou d'ingestion invalide"
            );
        }

        StopTimeEvent selected;
        String selectedType;

        if (stop.hasArrival() && complete(stop.getArrival())) {
            selected = stop.getArrival();
            selectedType = "arrival";
        } else if (stop.hasDeparture() && complete(stop.getDeparture())) {
            selected = stop.getDeparture();
            selectedType = "departure";
        } else {
            throw new IllegalArgumentException(
                    "Aucun horaire avec time et delay exploitables"
            );
        }

        if (selected.getTime() <= 0) {
            throw new IllegalArgumentException("Horaire estimé invalide");
        }

        Instant estimated = Instant.ofEpochSecond(selected.getTime());
        Instant scheduled = estimated.minusSeconds(selected.getDelay());

        String identity = identity(
                SOURCE,
                "1",
                trip.getTripId(),
                trip.getStartDate(),
                trip.getStartTime(),
                routeId,
                stop.getStopId(),
                stop.hasStopSequence()
                        ? Integer.toUnsignedString(stop.getStopSequence())
                        : "",
                selectedType,
                observationTimestamp.toString(),
                Long.toString(selected.getTime()),
                Integer.toString(selected.getDelay())
        );

        String eventId = UUID.nameUUIDFromBytes(
                identity.getBytes(StandardCharsets.UTF_8)
        ).toString();

        return new TripUpdateEvent(
                eventId,
                "trip_update",
                SOURCE,
                1,
                trip.getTripId(),
                routeId,
                stop.getStopId(),
                scheduled.toString(),
                estimated.toString(),
                selected.getDelay(),
                observationTimestamp.toString(),
                ingestedAt.toString()
        );
    }

    private static boolean complete(StopTimeEvent event) {
        return event.hasTime() && event.hasDelay();
    }

    private static String identity(String... parts) {
        StringBuilder result = new StringBuilder();

        for (String part : parts) {
            result.append(part.length()).append(':').append(part);
        }

        return result.toString();
    }
}