package fr.remyqueny.sncf.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.transit.realtime.GtfsRealtime.FeedMessage;
import com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeEvent;
import com.google.transit.realtime.GtfsRealtime.TripDescriptor;
import com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import java.util.TreeMap;

public final class GtfsRealtimeEnrichmentInspector {

    private GtfsRealtimeEnrichmentInspector() {
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            throw new IllegalArgumentException(
                    "Usage : GtfsRealtimeEnrichmentInspector "
                            + "<flux.pb> <referentiel.zip>"
            );
        }

        GtfsStaticReference reference =
                GtfsStaticReference.load(Path.of(args[1]));

        FeedMessage feed;
        try (InputStream input = Files.newInputStream(Path.of(args[0]))) {
            feed = FeedMessage.parseFrom(input);
        }

        if (!feed.getHeader().hasTimestamp()
                || feed.getHeader().getTimestamp() <= 0) {
            throw new IllegalArgumentException(
                    "Le flux doit fournir un timestamp "
                            + "de publication valide"
            );
        }

        Instant observationTimestamp =
                Instant.ofEpochSecond(feed.getHeader().getTimestamp());

        Instant ingestedAt = Instant.now();
        ObjectMapper objectMapper = new ObjectMapper();

        Map<String, Long> counts = new TreeMap<>();
        int examplesShown = 0;

        for (var entity : feed.getEntityList()) {
            if (!entity.hasTripUpdate()) {
                continue;
            }

            var update = entity.getTripUpdate();
            var trip = update.getTrip();

            if (trip.getScheduleRelationship()
                    != TripDescriptor.ScheduleRelationship.SCHEDULED) {
                increment(
                        counts,
                        "courses_exclues_"
                                + trip.getScheduleRelationship().name()
                );
                continue;
            }

            increment(counts, "courses_planifiees");

            String routeId =
                    reference.tripRoutes().get(trip.getTripId());

            var route = routeId == null
                    ? null
                    : reference.routes().get(routeId);

            if (route == null) {
                increment(counts, "courses_reference_non_resolue");
                continue;
            }

            increment(counts, "courses_enrichies");

            for (var stopUpdate : update.getStopTimeUpdateList()) {
                increment(counts, "mises_a_jour_arret_examinees");

                if (stopUpdate.getScheduleRelationship()
                        != StopTimeUpdate.ScheduleRelationship.SCHEDULED) {
                    increment(
                            counts,
                            "arrets_exclus_"
                                    + stopUpdate
                                            .getScheduleRelationship()
                                            .name()
                    );
                    continue;
                }

                var stop =
                        reference.stops().get(stopUpdate.getStopId());

                if (stop == null) {
                    increment(counts, "arrets_reference_non_resolue");
                    continue;
                }

                StopTimeEvent selected;
                String selectedType;

                if (stopUpdate.hasArrival()
                        && complete(stopUpdate.getArrival())) {
                    selected = stopUpdate.getArrival();
                    selectedType = "arrival";
                } else if (stopUpdate.hasDeparture()
                        && complete(stopUpdate.getDeparture())) {
                    selected = stopUpdate.getDeparture();
                    selectedType = "departure";
                } else {
                    increment(
                            counts,
                            "arrets_sans_time_et_delay_exploitables"
                    );
                    continue;
                }

                if (selected.getTime() <= 0) {
                    increment(counts, "arrets_timestamp_non_positif");
                    continue;
                }

                increment(counts, "arrets_candidats_au_mapping");
                increment(counts, "selection_" + selectedType);

                if (selected.getDelay() < 0) {
                    increment(counts, "retards_negatifs_selectionnes");
                }

                if (examplesShown < 2) {
                    TripUpdateEvent event = GtfsStopEventMapper.map(
                            update,
                            stopUpdate,
                            routeId,
                            observationTimestamp,
                            ingestedAt
                    );

                    System.out.println("\n--- JSON canonique ---");
                    System.out.println(
                            objectMapper.writerWithDefaultPrettyPrinter()
                                    .writeValueAsString(event)
                    );

                    examplesShown++;
                }
            }
        }

        System.out.println("\n--- Compteurs ---");
        counts.forEach((name, count) ->
                System.out.println(name + " : " + count)
        );
        System.out.println("Un compteur absent signifie zéro.");
    }

    private static boolean complete(StopTimeEvent event) {
        return event.hasTime() && event.hasDelay();
    }

    private static void increment(
            Map<String, Long> counts,
            String name
    ) {
        counts.merge(name, 1L, Long::sum);
    }
}