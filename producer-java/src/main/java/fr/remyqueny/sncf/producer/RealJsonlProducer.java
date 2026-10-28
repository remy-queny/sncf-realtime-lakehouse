package fr.remyqueny.sncf.producer;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public final class RealJsonlProducer {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(RealJsonlProducer.class);
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String TOPIC = "sncf.trip_updates.real.raw";
    private static final int MAX_EVENTS = 20000;

    private RealJsonlProducer() {
    }

    record Options(
            Path input,
            int count,
            String bootstrapServers,
            Path receiptPath,
            String expectedSha256
    ) {
    }

    public record Acknowledgement(
            @JsonProperty("event_id") String eventId,
            String topic,
            int partition,
            long offset
    ) {
    }

    public record PublicationReceipt(
            String topic,
            @JsonProperty("bootstrap_servers") String bootstrapServers,
            List<Acknowledgement> acknowledgements
    ) {
    }

    public static void main(String[] args) {
        try {
            run(args);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            LOGGER.warn("status=interrupted topic={}", TOPIC);
            System.exit(130);
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
        Options options = parseOptions(args);
        List<TripUpdateEvent> events = readEvents(
                options.input(), options.count(), options.expectedSha256()
        );
        validateReceiptDestination(options.receiptPath());

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, options.bootstrapServers());
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "sncf-real-jsonl-producer");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 30000);
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30000);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        LOGGER.info("status=preflight_ok topic={} event_count={}", TOPIC, events.size());

        try (KafkaProducer<String, String> kafka = new KafkaProducer<>(properties)) {
            publishEvents(events, options.bootstrapServers(), options.receiptPath(), kafka);
        }

        LOGGER.info("status=completed topic={} delivered_count={}", TOPIC, events.size());
        if (options.receiptPath() != null) {
            LOGGER.info("receipt_file={}", options.receiptPath());
        }
    }

    static Options parseOptions(String[] args) {
        if (args.length != 2 && args.length != 5) {
            throw new IllegalArgumentException(
                    "Usage : RealJsonlProducer fichier.jsonl nombre "
                            + "[bootstrap_servers reçu.json sha256_jsonl]"
            );
        }
        int count = Integer.parseInt(args[1]);
        validateCount(count);
        String bootstrap = args.length == 5
                ? args[2] : AppConfig.fromEnvironment().bootstrapServers();
        if (bootstrap == null || bootstrap.isBlank()) {
            throw new IllegalArgumentException("Adresse bootstrap absente.");
        }
        String expectedHash = args.length == 5 ? args[4] : null;
        if (expectedHash != null && !expectedHash.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("Empreinte SHA-256 invalide.");
        }
        return new Options(
                Path.of(args[0]), count, bootstrap,
                args.length == 5 ? Path.of(args[3]).toAbsolutePath().normalize() : null,
                expectedHash
        );
    }

    static List<TripUpdateEvent> readEvents(
            Path input, int requestedCount, String expectedSha256
    ) throws IOException {
        validateCount(requestedCount);
        checkInputHash(input, expectedSha256);
        List<TripUpdateEvent> events = new ArrayList<>();
        HashSet<String> eventIds = new HashSet<>();

        try (BufferedReader reader = Files.newBufferedReader(input, StandardCharsets.UTF_8)) {
            String line;
            while (events.size() < requestedCount && (line = reader.readLine()) != null) {
                TripUpdateEvent event = JSON.readValue(line, TripUpdateEvent.class);
                validate(event);
                if (!eventIds.add(event.eventId())) {
                    throw new IllegalArgumentException("event_id dupliqué dans la sélection.");
                }
                events.add(event);
            }
        }
        if (events.size() != requestedCount) {
            throw new IllegalArgumentException("Le fichier ne contient pas assez d'événements.");
        }
        checkInputHash(input, expectedSha256);
        return List.copyOf(events);
    }

    static PublicationReceipt publishEvents(
            List<TripUpdateEvent> events,
            String bootstrapServers,
            Path receiptPath,
            Producer<String, String> producer
    ) throws IOException, InterruptedException, ExecutionException {
        Objects.requireNonNull(events);
        Objects.requireNonNull(producer);
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Interruption demandée avant la publication.");
        }
        validateCount(events.size());
        if (bootstrapServers == null || bootstrapServers.isBlank()) {
            throw new IllegalArgumentException("Adresse bootstrap absente.");
        }

        HashSet<String> eventIds = new HashSet<>();
        List<String> payloads = new ArrayList<>();
        for (TripUpdateEvent event : events) {
            validate(event);
            if (!eventIds.add(event.eventId())) {
                throw new IllegalArgumentException("event_id dupliqué dans la sélection.");
            }
            payloads.add(JSON.writeValueAsString(event));
        }

        validateReceiptDestination(receiptPath);
        Path output = receiptPath == null ? null : receiptPath.toAbsolutePath().normalize();
        Path partial = output == null ? null : output.resolveSibling(output.getFileName() + ".partial");
        if (output != null) {
            Files.createDirectories(output.getParent());
        }
        List<Acknowledgement> acknowledgements = new ArrayList<>();
        HashSet<String> positions = new HashSet<>();
        PublicationReceipt receipt;

        try (FileChannel channel = partial == null ? null : FileChannel.open(
                partial, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE
        )) {
            for (int index = 0; index < events.size(); index++) {
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedException("Interruption demandée avant l'envoi.");
                }
                TripUpdateEvent event = events.get(index);
                RecordMetadata metadata = producer.send(
                        new ProducerRecord<>(TOPIC, event.tripId(), payloads.get(index))
                ).get();

                if (!TOPIC.equals(metadata.topic())
                        || metadata.partition() < 0 || metadata.offset() < 0
                        || !positions.add(metadata.partition() + ":" + metadata.offset())) {
                    throw new IllegalStateException("Métadonnées Kafka incohérentes.");
                }
                acknowledgements.add(new Acknowledgement(
                        event.eventId(), metadata.topic(), metadata.partition(), metadata.offset()
                ));
                LOGGER.info(
                        "status=delivered event_id={} topic={} partition={} offset={}",
                        event.eventId(), metadata.topic(), metadata.partition(), metadata.offset()
                );
            }

            receipt = new PublicationReceipt(
                    TOPIC, bootstrapServers, List.copyOf(acknowledgements)
            );
            if (channel != null) {
                ByteBuffer bytes = ByteBuffer.wrap(JSON.writeValueAsBytes(receipt));
                while (bytes.hasRemaining()) {
                    channel.write(bytes);
                }
                channel.force(true);
            }
        }

        if (output != null) {
            Files.move(partial, output);
        }
        return receipt;
    }

    private static void validateReceiptDestination(Path receiptPath) throws IOException {
        if (receiptPath == null) {
            return;
        }
        Path output = receiptPath.toAbsolutePath().normalize();
        Path partial = output.resolveSibling(output.getFileName() + ".partial");
        for (Path path : List.of(output, partial)) {
            if (Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
                throw new FileAlreadyExistsException(path.toString());
            }
        }
    }

    private static void checkInputHash(Path input, String expected) throws IOException {
        if (expected != null
                && (!expected.matches("[0-9a-f]{64}") || !expected.equals(sha256(input)))) {
            throw new IllegalArgumentException("L'empreinte du JSONL ne correspond pas au lot préparé.");
        }
    }

    static String sha256(Path input) throws IOException {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException(exception);
        }
        try (InputStream stream = Files.newInputStream(input)) {
            byte[] buffer = new byte[8192];
            int count;
            while ((count = stream.read(buffer)) != -1) {
                digest.update(buffer, 0, count);
            }
        }
        return java.util.HexFormat.of().formatHex(digest.digest());
    }

    private static void validateCount(int count) {
        if (count < 1 || count > MAX_EVENTS) {
            throw new IllegalArgumentException("Le nombre doit être compris entre 1 et " + MAX_EVENTS);
        }
    }

    private static void validate(TripUpdateEvent event) {
        if (event == null || !"sncf_gtfs_rt".equals(event.source())
                || !"trip_update".equals(event.eventType()) || event.schemaVersion() != 1) {
            throw new IllegalArgumentException("Événement réel de version 1 attendu.");
        }
        if (blank(event.eventId()) || blank(event.tripId())
                || blank(event.routeId()) || blank(event.stopId())) {
            throw new IllegalArgumentException("Identifiant obligatoire absent.");
        }
        UUID.fromString(event.eventId());
        Instant scheduled = Instant.parse(event.scheduledTimestamp());
        Instant estimated = Instant.parse(event.estimatedTimestamp());
        Instant.parse(event.eventTimestamp());
        Instant.parse(event.ingestedAt());
        if (!scheduled.plusSeconds(event.delaySeconds()).equals(estimated)) {
            throw new IllegalArgumentException("Horaires incompatibles avec delay_seconds.");
        }
    }

    private static boolean blank(String value) {
        return value == null || value.isBlank();
    }
}