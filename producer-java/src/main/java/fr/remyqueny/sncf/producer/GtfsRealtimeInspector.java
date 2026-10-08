package fr.remyqueny.sncf.producer;

import com.google.transit.realtime.GtfsRealtime.FeedEntity;
import com.google.transit.realtime.GtfsRealtime.FeedMessage;
import com.google.transit.realtime.GtfsRealtime.TripUpdate;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import java.util.TreeMap;

public final class GtfsRealtimeInspector {

    private GtfsRealtimeInspector() {
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            throw new IllegalArgumentException(
                    "Usage : GtfsRealtimeInspector <fichier.pb>"
            );
        }

        Path path = Path.of(args[0]);
        FeedMessage feed;

        try (InputStream input = Files.newInputStream(path)) {
            feed = FeedMessage.parseFrom(input);
        }

        int tripUpdateCount = 0;
        long stopUpdateCount = 0;
        int absentRelationshipCount = 0;
        Map<String, Integer> relationships = new TreeMap<>();

        for (FeedEntity entity : feed.getEntityList()) {
            if (!entity.hasTripUpdate()) {
                continue;
            }

            TripUpdate update = entity.getTripUpdate();
            tripUpdateCount++;
            stopUpdateCount += update.getStopTimeUpdateCount();

            if (!update.getTrip().hasScheduleRelationship()) {
                absentRelationshipCount++;
            }

            String relationship = update.getTrip()
                    .getScheduleRelationship()
                    .name();

            relationships.merge(relationship, 1, Integer::sum);
        }

        System.out.println("Fichier : " + path.toAbsolutePath());
        System.out.println(
                "Version GTFS-RT : "
                        + feed.getHeader().getGtfsRealtimeVersion()
        );
        System.out.println("Nombre d'entités : " + feed.getEntityCount());
        System.out.println("Entités TripUpdate : " + tripUpdateCount);
        System.out.println("Mises à jour d'arrêt : " + stopUpdateCount);

        if (feed.getHeader().hasTimestamp()) {
            System.out.println(
                    "Timestamp du flux UTC : "
                            + Instant.ofEpochSecond(
                                    feed.getHeader().getTimestamp()
                            )
            );
        } else {
            System.out.println("Timestamp du flux : absent");
        }

        System.out.println("Statuts interprétés : " + relationships);
        System.out.println(
                "schedule_relationship non renseigné : "
                        + absentRelationshipCount
        );

        int shown = 0;

        for (FeedEntity entity : feed.getEntityList()) {
            if (!entity.hasTripUpdate()) {
                continue;
            }

            TripUpdate update = entity.getTripUpdate();

            System.out.println("\n--- Exemple ---");
            System.out.println("Identifiant d'entité : " + entity.getId());
            System.out.println(update.getTrip());

            int limit = Math.min(2, update.getStopTimeUpdateCount());
            for (int index = 0; index < limit; index++) {
                System.out.println(update.getStopTimeUpdate(index));
            }

            shown++;
            if (shown == 2) {
                break;
            }
        }
    }
}