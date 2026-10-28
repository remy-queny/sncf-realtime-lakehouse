package fr.remyqueny.sncf.producer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RealJsonlProducerTest {

    private static final String BOOTSTRAP = "localhost:9092";
    private static final String TOPIC = "sncf.tripupdates.real.raw";
    private static final ObjectMapper JSON = new ObjectMapper();

    @TempDir
    Path directory;

    @Test
    void shouldReadRequestedPrefixAndCheckHash() throws Exception {
        Path input = writeEvents(event(0), event(1), event(2));
        var selected = RealJsonlProducer.readEvents(input, 2, RealJsonlProducer.sha256(input));
        assertEquals(List.of(event(0), event(1)), selected);
    }

    @Test
    void shouldRejectWrongHashBeforePublishing() throws Exception {
        Path input = writeEvents(event(0));
        assertThrows(IllegalArgumentException.class,
                () -> RealJsonlProducer.readEvents(input, 1, "0".repeat(64)));
    }

    @Test
    void shouldRejectDuplicateIdsBeforePublishing() throws Exception {
        try (var kafka = mock()) {
            Path output = directory.resolve("receipt.json");
            assertThrows(IllegalArgumentException.class,
                    () -> RealJsonlProducer.publishEvents(
                            List.of(event(0), event(0)), BOOTSTRAP, output, kafka));
            assertTrue(kafka.history().isEmpty());
            assertFalse(Files.exists(output));
            assertFalse(Files.exists(directory.resolve("receipt.json.partial")));
        }
    }

    @Test
    void shouldRejectInconsistentDelayInInput() throws Exception {
        TripUpdateEvent event = modified(event(0), "sncf_gtfs_rt", "2026-10-28T15:02:01Z");
        Path input = writeEvents(event);
        assertThrows(IllegalArgumentException.class,
                () -> RealJsonlProducer.readEvents(input, 1, null));
    }

    @Test
    void shouldWriteReceiptForAllAcknowledgedEvents() throws Exception {
        try (var kafka = mock()) {
            Path output = directory.resolve("receipts/batch.json");
            var receipt = RealJsonlProducer.publishEvents(
                    List.of(event(0), event(1)), BOOTSTRAP, output, kafka);
            assertEquals(2, receipt.acknowledgements().size());
            assertEquals(2, kafka.history().size());
            assertEquals(event(0).tripId(), kafka.history().get(0).key());
            assertEquals(TOPIC, kafka.history().get(0).topic());
            JsonNode node = JSON.readTree(output.toFile());
            assertEquals(TOPIC, node.path("topic").asText());
            assertEquals(BOOTSTRAP, node.path("bootstrap_servers").asText());
            assertEquals(2, node.path("acknowledgements").size());
            JsonNode first = node.path("acknowledgements").get(0);
            assertEquals(event(0).eventId(), first.path("event_id").asText());
            assertEquals(0, first.path("partition").asInt());
            assertEquals(0L, first.path("offset").asLong());
            assertEquals(1L, node.path("acknowledgements").get(1).path("offset").asLong());
            assertFalse(Files.exists(output.resolveSibling("batch.json.partial")));
        }
    }

    @Test
    void shouldNotOverwriteExistingReceipt() throws Exception {
        Path output = directory.resolve("receipt.json");
        byte[] before = "existing-receipt".getBytes(StandardCharsets.UTF_8);
        Files.write(output, before);
        try (var kafka = mock()) {
            assertThrows(FileAlreadyExistsException.class,
                    () -> RealJsonlProducer.publishEvents(List.of(event(0)), BOOTSTRAP, output, kafka));
            assertTrue(kafka.history().isEmpty());
            assertArrayEquals(before, Files.readAllBytes(output));
        }
    }

    @Test
    void shouldNotOverwriteExistingPartialReceipt() throws Exception {
        Path output = directory.resolve("receipt.json");
        Path partial = directory.resolve("receipt.json.partial");
        Files.writeString(partial, "partial-attempt");
        try (var kafka = mock()) {
            assertThrows(FileAlreadyExistsException.class,
                    () -> RealJsonlProducer.publishEvents(List.of(event(0)), BOOTSTRAP, output, kafka));
            assertTrue(kafka.history().isEmpty());
            assertEquals("partial-attempt", Files.readString(partial));
            assertFalse(Files.exists(output));
        }
    }

    @Test
    void shouldNotCreateFinalReceiptAfterPartialSendFailure() throws Exception {
        try (var kafka = new MockProducer<String, String>(
                true, null, new StringSerializer(), new StringSerializer()) {
            private int calls;
            @Override
            public Future<RecordMetadata> send(ProducerRecord<String, String> record) {
                if (++calls == 2) {
                    return CompletableFuture.failedFuture(new KafkaException("Échec simulé"));
                }
                return super.send(record);
            }
        }) {
            Path output = directory.resolve("receipt.json");
            assertThrows(ExecutionException.class,
                    () -> RealJsonlProducer.publishEvents(
                            List.of(event(0), event(1)), BOOTSTRAP, output, kafka));
            assertEquals(1, kafka.history().size());
            assertFalse(Files.exists(output));
            assertTrue(Files.exists(directory.resolve("receipt.json.partial")));
        }
    }

    @Test
    void shouldPreserveLegacyModeWithoutReceiptFile() throws Exception {
        try (var kafka = mock()) {
            var receipt = RealJsonlProducer.publishEvents(List.of(event(0)), BOOTSTRAP, null, kafka);
            assertEquals(1, kafka.history().size());
            assertEquals(1, receipt.acknowledgements().size());
            try (var files = Files.list(directory)) {
                assertEquals(0L, files.count());
            }
        }
    }

    @Test
    void shouldStopBeforeSendingWhenInterrupted() throws Exception {
        try (var kafka = mock()) {
            Thread.currentThread().interrupt();
            try {
                assertThrows(InterruptedException.class,
                        () -> RealJsonlProducer.publishEvents(
                                List.of(event(0)), BOOTSTRAP, directory.resolve("receipt.json"), kafka));
                assertTrue(kafka.history().isEmpty());
                assertFalse(Files.exists(directory.resolve("receipt.json")));
            } finally {
                Thread.interrupted();
            }
        }
    }

    @Test
    void shouldRejectInsufficientEvents() throws Exception {
        Path input = writeEvents(event(0));
        assertThrows(IllegalArgumentException.class,
                () -> RealJsonlProducer.readEvents(input, 2, null));
    }

    @Test
    void shouldRejectCountOutsideBounds() {
        for (int count : new int[]{0, -1, 20001}) {
            assertThrows(IllegalArgumentException.class,
                    () -> RealJsonlProducer.readEvents(directory.resolve("absent.jsonl"), count, null));
        }
    }

    @Test
    void shouldRejectForeignSource() throws Exception {
        Path input = writeEvents(modified(event(0), "simulation_java", "2026-10-28T15:02:00Z"));
        assertThrows(IllegalArgumentException.class,
                () -> RealJsonlProducer.readEvents(input, 1, null));
    }

    @Test
    void shouldRejectBlankBootstrapBeforeSending() throws Exception {
        try (var kafka = mock()) {
            assertThrows(IllegalArgumentException.class,
                    () -> RealJsonlProducer.publishEvents(
                            List.of(event(0)), " ", directory.resolve("receipt.json"), kafka));
            assertTrue(kafka.history().isEmpty());
        }
    }

    @Test
    void shouldRejectAcknowledgementForWrongTopic() throws Exception {
        try (var kafka = new MockProducer<String, String>(
                true, null, new StringSerializer(), new StringSerializer()) {
            @Override
            public Future<RecordMetadata> send(ProducerRecord<String, String> record) {
                return super.send(new ProducerRecord<>("wrong-topic", record.key(), record.value()));
            }
        }) {
            Path output = directory.resolve("receipt.json");
            assertThrows(IllegalStateException.class,
                    () -> RealJsonlProducer.publishEvents(List.of(event(0)), BOOTSTRAP, output, kafka));
            assertFalse(Files.exists(output));
        }
    }

    @Test
    void shouldParseLegacyAndReceiptArgumentsWithoutSending() {
        var legacy = RealJsonlProducer.parseOptions(new String[]{"events.jsonl", "2"});
        assertEquals(2, legacy.count());
        assertNull(legacy.receiptPath());
        assertNull(legacy.expectedSha256());
        var tracked = RealJsonlProducer.parseOptions(new String[]{
                "events.jsonl", "2", BOOTSTRAP, "receipt.json", "a".repeat(64)
        });
        assertEquals(BOOTSTRAP, tracked.bootstrapServers());
        assertEquals("a".repeat(64), tracked.expectedSha256());
        assertEquals(Path.of("receipt.json").toAbsolutePath().normalize(), tracked.receiptPath());
        assertThrows(IllegalArgumentException.class,
                () -> RealJsonlProducer.parseOptions(new String[]{"events.jsonl", "2", BOOTSTRAP}));
    }

    private static MockProducer<String, String> mock() {
        return new MockProducer<>(true, null, new StringSerializer(), new StringSerializer());
    }

    private Path writeEvents(TripUpdateEvent... events) throws IOException {
        Path input = directory.resolve("events.jsonl");
        StringBuilder lines = new StringBuilder();
        for (TripUpdateEvent event : events) {
            lines.append(JSON.writeValueAsString(event)).append('\n');
        }
        Files.writeString(input, lines.toString(), StandardCharsets.UTF_8);
        return input;
    }

    private static TripUpdateEvent event(int index) {
        return new TripUpdateEvent(
                UUID.nameUUIDFromBytes(("event-" + index).getBytes(StandardCharsets.UTF_8)).toString(),
                "trip_update", "sncf_gtfs_rt", 1,
                "trip-one", "route-one", "stop-one",
                "2026-10-28T15:00:00Z", "2026-10-28T15:02:00Z", 120,
                "2026-10-28T14:59:00Z", "2026-10-28T14:59:01Z"
        );
    }

    private static TripUpdateEvent modified(TripUpdateEvent event, String source, String estimated) {
        return new TripUpdateEvent(
                event.eventId(), event.eventType(), source, event.schemaVersion(),
                event.tripId(), event.routeId(), event.stopId(),
                event.scheduledTimestamp(), estimated, event.delaySeconds(),
                event.eventTimestamp(), event.ingestedAt()
        );
    }
}