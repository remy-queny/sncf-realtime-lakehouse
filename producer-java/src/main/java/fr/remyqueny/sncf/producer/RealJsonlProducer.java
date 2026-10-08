package fr.remyqueny.sncf.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public final class RealJsonlProducer {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(RealJsonlProducer.class);

    private static final String TOPIC = "sncf.trip_updates.real.raw";
    private static final int MAX_EVENTS = 20_000;

    private RealJsonlProducer() {
    }

    public static void main(String[] args) {
        try {
            run(args);
        } catch (Exception exception) {
            LOGGER.error(
                    "status=failed topic={} exception_type={} message={}",
                    TOPIC,
                    exception.getClass().getSimpleName(),
                    exception.getMessage(),
                    exception
            );
            System.exit(1);
        }
    }

    private static void run(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException(
                    "Usage : RealJsonlProducer <fichier.jsonl> <nombre>"
            );
        }

        Path input = Path.of(args[0]);
        int requestedCount = Integer.parseInt(args[1]);

        if (requestedCount < 1 || requestedCount > MAX_EVENTS) {
            throw new IllegalArgumentException(
                    "Le nombre doit être compris entre 1 et " + MAX_EVENTS
            );
        }

        ObjectMapper objectMapper = new ObjectMapper();
        List<TripUpdateEvent> events = new ArrayList<>();
        HashSet<String> eventIds = new HashSet<>();

        try (BufferedReader reader = Files.newBufferedReader(
                input,
                StandardCharsets.UTF_8
        )) {
            String line;

            while (events.size() < requestedCount
                    && (line = reader.readLine()) != null) {
                TripUpdateEvent event =
                        objectMapper.readValue(line, TripUpdateEvent.class);

                validate(event);

                if (!eventIds.add(event.eventId())) {
                    throw new IllegalArgumentException(
                            "event_id dupliqué dans la sélection : "
                                    + event.eventId()
                    );
                }

                events.add(event);
            }
        }

        if (events.size() != requestedCount) {
            throw new IllegalArgumentException(
                    "Le fichier ne contient pas assez d'événements"
            );
        }

        String bootstrapServers =
                AppConfig.fromEnvironment().bootstrapServers();

        Properties properties = new Properties();
        properties.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers
        );
        properties.put(
                ProducerConfig.CLIENT_ID_CONFIG,
                "sncf-real-jsonl-producer"
        );
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 30_000);
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30_000);
        properties.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName()
        );
        properties.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName()
        );

        LOGGER.info(
                "status=preflight_ok topic={} event_count={}",
                TOPIC,
                events.size()
        );

        int deliveredCount = 0;

        try (KafkaProducer<String, String> producer =
                     new KafkaProducer<>(properties)) {
            for (TripUpdateEvent event : events) {
                ProducerRecord<String, String> record =
                        new ProducerRecord<>(
                                TOPIC,
                                event.tripId(),
                                objectMapper.writeValueAsString(event)
                        );

                var metadata = producer.send(record).get();
                deliveredCount++;

                LOGGER.info(
                        "status=delivered event_id={} topic={} "
                                + "partition={} offset={}",
                        event.eventId(),
                        metadata.topic(),
                        metadata.partition(),
                        metadata.offset()
                );
            }
        }

        LOGGER.info(
                "status=completed topic={} delivered_count={}",
                TOPIC,
                deliveredCount
        );
    }

    private static void validate(TripUpdateEvent event) {
        if (event == null
                || !"sncf_gtfs_rt".equals(event.source())
                || !"trip_update".equals(event.eventType())
                || event.schemaVersion() != 1) {
            throw new IllegalArgumentException(
                    "Événement réel de version 1 attendu"
            );
        }

        if (blank(event.eventId())
                || blank(event.tripId())
                || blank(event.routeId())
                || blank(event.stopId())) {
            throw new IllegalArgumentException(
                    "Identifiant obligatoire absent"
            );
        }

        UUID.fromString(event.eventId());

        Instant scheduled = Instant.parse(event.scheduledTimestamp());
        Instant estimated = Instant.parse(event.estimatedTimestamp());
        Instant.parse(event.eventTimestamp());
        Instant.parse(event.ingestedAt());

        if (!scheduled.plusSeconds(event.delaySeconds()).equals(estimated)) {
            throw new IllegalArgumentException(
                    "Horaires incompatibles avec delay_seconds"
            );
        }
    }

    private static boolean blank(String value) {
        return value == null || value.isBlank();
    }
}