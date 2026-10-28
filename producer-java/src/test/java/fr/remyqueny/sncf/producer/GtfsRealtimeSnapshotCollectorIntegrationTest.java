package fr.remyqueny.sncf.producer;

import com.google.transit.realtime.GtfsRealtime.FeedEntity;
import com.google.transit.realtime.GtfsRealtime.FeedHeader;
import com.google.transit.realtime.GtfsRealtime.FeedMessage;
import com.google.transit.realtime.GtfsRealtime.TripDescriptor;
import com.google.transit.realtime.GtfsRealtime.TripUpdate;
import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GtfsRealtimeSnapshotCollectorIntegrationTest {

    private static final Instant PUBLISHED_AT =
            Instant.parse("2026-10-28T10:00:00Z");

    @TempDir
    Path directory;

    private HttpServer server;

    private volatile Response response =
            new Response(200, feed("entity-one"));

    private record Response(int status, byte[] body) {
    }

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(
                new InetSocketAddress("127.0.0.1", 0),
                0
        );

        server.createContext("/feed", exchange -> {
            Response current = response;

            try {
                exchange.getResponseHeaders().set(
                        "Content-Type",
                        "application/x-protobuf"
                );

                exchange.sendResponseHeaders(
                        current.status(),
                        current.body().length
                );

                try (OutputStream output = exchange.getResponseBody()) {
                    output.write(current.body());
                }
            } finally {
                exchange.close();
            }
        });

        server.start();
    }

    @AfterEach
    void stopServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void shouldKeepOneArchiveForRepeatedHttpSnapshotAfterRestart()
            throws Exception {

        byte[] body = feed("entity-one");
        response = new Response(200, body);

        var firstCollector = new GtfsRealtimeSnapshotCollector();
        var first = firstCollector.collect(source(), directory);

        Path entryPath = directory.resolve("registry")
                .resolve(first.sha256() + ".json");

        byte[] originalEntry = Files.readAllBytes(entryPath);

        var restartedCollector = new GtfsRealtimeSnapshotCollector();
        var second = restartedCollector.collect(source(), directory);

        assertTrue(first.newSnapshot());
        assertFalse(second.newSnapshot());
        assertEquals(first.sha256(), second.sha256());
        assertEquals(first.publishedAt(), second.publishedAt());
        assertNull(second.archivedFile());

        assertArrayEquals(
                body,
                Files.readAllBytes(first.archivedFile())
        );

        assertArrayEquals(
                originalEntry,
                Files.readAllBytes(entryPath)
        );

        assertArchiveCount(1);
        assertRegistryEntryCount(1);
    }

    @Test
    void shouldKeepChangedHttpSnapshotWithSamePublishedTimestamp()
            throws Exception {

        var collector = new GtfsRealtimeSnapshotCollector();

        response = new Response(200, feed("entity-one"));
        var first = collector.collect(source(), directory);

        response = new Response(200, feed("entity-two"));
        var second = collector.collect(source(), directory);

        assertTrue(first.newSnapshot());
        assertTrue(second.newSnapshot());
        assertEquals(PUBLISHED_AT, first.publishedAt());
        assertEquals(first.publishedAt(), second.publishedAt());
        assertNotEquals(first.sha256(), second.sha256());

        assertTrue(Files.isRegularFile(first.archivedFile()));
        assertTrue(Files.isRegularFile(second.archivedFile()));

        assertArchiveCount(2);
        assertRegistryEntryCount(2);
    }

    @Test
    void shouldPreserveExistingCollectionAfterHttpError()
            throws Exception {

        var collector = new GtfsRealtimeSnapshotCollector();

        byte[] body = feed("entity-one");
        response = new Response(200, body);

        var first = collector.collect(source(), directory);

        Path entryPath = directory.resolve("registry")
                .resolve(first.sha256() + ".json");

        byte[] originalEntry = Files.readAllBytes(entryPath);

        response = new Response(
                503,
                "Service unavailable".getBytes(StandardCharsets.UTF_8)
        );

        IOException exception = assertThrows(
                IOException.class,
                () -> collector.collect(source(), directory)
        );

        assertTrue(exception.getMessage().contains("503"));

        assertArrayEquals(
                body,
                Files.readAllBytes(first.archivedFile())
        );

        assertArrayEquals(
                originalEntry,
                Files.readAllBytes(entryPath)
        );

        assertArchiveCount(1);
        assertRegistryEntryCount(1);

        try (var paths = Files.walk(directory)) {
            assertFalse(paths.anyMatch(
                    path -> path.getFileName()
                            .toString()
                            .endsWith(".partial")
            ));
        }
    }

    private URI source() {
        return URI.create(
                "http://127.0.0.1:"
                        + server.getAddress().getPort()
                        + "/feed"
        );
    }

    private static byte[] feed(String entityId) {
        return FeedMessage.newBuilder()
                .setHeader(
                        FeedHeader.newBuilder()
                                .setGtfsRealtimeVersion("1.0")
                                .setTimestamp(PUBLISHED_AT.getEpochSecond())
                )
                .addEntity(
                        FeedEntity.newBuilder()
                                .setId(entityId)
                                .setTripUpdate(
                                        TripUpdate.newBuilder()
                                                .setTrip(
                                                        TripDescriptor
                                                                .newBuilder()
                                                                .setTripId(
                                                                        "trip-test"
                                                                )
                                                )
                                )
                )
                .build()
                .toByteArray();
    }

    private void assertArchiveCount(long expected) throws IOException {
        try (var paths = Files.walk(directory.resolve("snapshots"))) {
            long count = paths
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName()
                            .toString()
                            .equals("tripupdates.pb"))
                    .count();

            assertEquals(expected, count);
        }
    }

    private void assertRegistryEntryCount(long expected)
            throws IOException {

        try (var paths = Files.list(directory.resolve("registry"))) {
            long count = paths
                    .filter(path -> path.getFileName()
                            .toString()
                            .endsWith(".json"))
                    .count();

            assertEquals(expected, count);
        }
    }
}