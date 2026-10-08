package fr.remyqueny.sncf.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.transit.realtime.GtfsRealtime.FeedMessage;
import com.google.transit.realtime.GtfsRealtime.TripDescriptor;
import com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;

public final class GtfsRealtimeJsonExporter {

    private GtfsRealtimeJsonExporter() {
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            throw new IllegalArgumentException(
                    "Usage : GtfsRealtimeJsonExporter "
                            + "<flux.pb> <referentiel.zip> <sortie.jsonl>"
            );
        }

        Path output = Path.of(args[2]);
        Path partial = output.resolveSibling(
                output.getFileName().toString() + ".partial"
        );

        if (Files.exists(output) || Files.exists(partial)) {
            throw new IOException(
                    "La sortie ou son fichier .partial existe déjà. "
                            + "Choisir un nouveau nom."
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
                    "Timestamp de publication du flux absent ou invalide"
            );
        }

        Instant observationTimestamp =
                Instant.ofEpochSecond(feed.getHeader().getTimestamp());

        Instant ingestedAt = Instant.now();
        ObjectMapper objectMapper = new ObjectMapper();

        Map<String, Long> counts = new TreeMap<>();
        HashSet<String> eventIds = new HashSet<>();

        try (BufferedWriter writer = Files.newBufferedWriter(
                partial,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE
        )) {
            for (var entity : feed.getEntityList()) {
                if (!entity.hasTripUpdate()) {
                    continue;
                }

                var update = entity.getTripUpdate();
                var trip = update.getTrip();
                int stopCount = update.getStopTimeUpdateCount();

                add(counts, "courses_source", 1);
                add(counts, "arrets_source", stopCount);

                if (trip.getScheduleRelationship()
                        != TripDescriptor.ScheduleRelationship.SCHEDULED) {
                    add(
                            counts,
                            "courses_exclues_"
                                    + trip.getScheduleRelationship().name(),
                            1
                    );
                    add(counts, "arrets_exclus_avec_course", stopCount);
                    continue;
                }

                String routeId =
                        reference.tripRoutes().get(trip.getTripId());

                var route = routeId == null
                        ? null
                        : reference.routes().get(routeId);

                if (route == null) {
                    add(counts, "courses_reference_non_resolue", 1);
                    add(counts, "arrets_exclus_avec_course", stopCount);
                    continue;
                }

                add(counts, "courses_enrichies", 1);

                for (var stop : update.getStopTimeUpdateList()) {
                    if (stop.getScheduleRelationship()
                            != StopTimeUpdate.ScheduleRelationship.SCHEDULED) {
                        add(
                                counts,
                                "arrets_exclus_"
                                        + stop.getScheduleRelationship().name(),
                                1
                        );
                        add(counts, "arrets_exclus_individuellement", 1);
                        continue;
                    }

                    if (!reference.stops().containsKey(stop.getStopId())) {
                        add(counts, "arrets_reference_non_resolue", 1);
                        add(counts, "arrets_exclus_individuellement", 1);
                        continue;
                    }

                    TripUpdateEvent event = GtfsStopEventMapper.map(
                            update,
                            stop,
                            routeId,
                            observationTimestamp,
                            ingestedAt
                    );

                    if (!eventIds.add(event.eventId())) {
                        throw new IllegalStateException(
                                "event_id dupliqué : " + event.eventId()
                        );
                    }

                    writer.write(objectMapper.writeValueAsString(event));
                    writer.newLine();
                    add(counts, "evenements_exportes", 1);
                }
            }

            long accountedStops =
                    counts.getOrDefault("evenements_exportes", 0L)
                            + counts.getOrDefault(
                                    "arrets_exclus_avec_course", 0L
                            )
                            + counts.getOrDefault(
                                    "arrets_exclus_individuellement", 0L
                            );

            if (accountedStops != counts.getOrDefault("arrets_source", 0L)) {
                throw new IllegalStateException(
                        "Bilan incohérent des mises à jour d'arrêt"
                );
            }
        }

        Files.move(partial, output);

        System.out.println("\n--- Bilan de l'export ---");
        counts.forEach((name, count) ->
                System.out.println(name + " : " + count)
        );
        System.out.println("event_id distincts : " + eventIds.size());
        System.out.println("Sortie : " + output.toAbsolutePath());
        System.out.println("Export terminé sans envoi Kafka.");
    }

    private static void add(
            Map<String, Long> counts,
            String name,
            long amount
    ) {
        counts.merge(name, amount, Long::sum);
    }
}