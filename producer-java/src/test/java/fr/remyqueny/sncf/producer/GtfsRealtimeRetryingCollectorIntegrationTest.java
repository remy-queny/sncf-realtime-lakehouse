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
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GtfsRealtimeRetryingCollectorIntegrationTest {

    private static final Instant PUBLISHED_AT =
            Instant.parse("2026-10-28T10:00:00Z");
    private static final Clock CLOCK =
            Clock.fixed(PUBLISHED_AT, ZoneOffset.UTC);

    @TempDir
    Path directory;

    private HttpServer server;
    private final AtomicInteger requests = new AtomicInteger();
    private volatile List<Response> plan = List.of(success());

    private record Response(int status, byte[] body, String retryAfter) {
    }

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(
                new InetSocketAddress("127.0.0.1", 0),
                0
        );

        server.createContext("/feed", exchange -> {
            int index = requests.getAndIncrement();
            List<Response> currentPlan = plan;
            Response current = currentPlan.get(
                    Math.min(index, currentPlan.size() - 1)
            );

            try {
                exchange.getResponseHeaders().set(
                        "Content-Type", "application/x-protobuf"
                );

                if (current.retryAfter() != null) {
                    exchange.getResponseHeaders().set(
                            "Retry-After", current.retryAfter()
                    );
                }

                exchange.sendResponseHeaders(
                        current.status(), current.body().length
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
    void shouldRecoverFrom503AndRegisterOneSnapshot() throws Exception {
        plan = List.of(failure(503, null), success());
        List<Long> waits = new ArrayList<>();

        var result = collector(waits).collect(source(), directory);

        assertTrue(result.newSnapshot());
        assertTrue(Files.isRegularFile(result.archivedFile()));
        assertEquals(2, requests.get());
        assertEquals(List.of(5000L), waits);
        assertArchiveCount(1);
        assertRegistryEntryCount(1);
        assertNoPartialFiles();
    }

    @Test
    void shouldHonorRetryAfterFromRealHttpResponse() throws Exception {
        plan = List.of(failure(429, "120"), success());
        List<Long> waits = new ArrayList<>();

        var result = collector(waits).collect(source(), directory);

        assertTrue(result.newSnapshot());
        assertEquals(2, requests.get());
        assertEquals(List.of(120000L), waits);
        assertArchiveCount(1);
        assertRegistryEntryCount(1);
        assertNoPartialFiles();
    }

    @Test
    void shouldStopAfterThree503ResponsesWithoutRegistration() throws Exception {
        plan = List.of(failure(503, null));
        List<Long> waits = new ArrayList<>();

        var exception = assertThrows(
                GtfsRealtimeDownloadException.class,
                () -> collector(waits).collect(source(), directory)
        );

        assertEquals(503, exception.statusCode());
        assertEquals(3, requests.get());
        assertEquals(List.of(5000L, 10000L), waits);
        assertFalse(Files.exists(directory.resolve("registry")));

        assertArchiveCount(0);
        assertNoPartialFiles();
    }

    @Test
    void shouldNotRetry401Response() throws Exception {
        plan = List.of(failure(401, null));
        List<Long> waits = new ArrayList<>();

        var exception = assertThrows(
                GtfsRealtimeDownloadException.class,
                () -> collector(waits).collect(source(), directory)
        );

        assertEquals(401, exception.statusCode());
        assertEquals(1, requests.get());
        assertEquals(List.of(), waits);
        assertFalse(Files.exists(directory.resolve("registry")));
        assertArchiveCount(0);
        assertNoPartialFiles();
    }

    @Test
    void shouldNotRepeatHttpRequestAfterLocalRegistryFailure()
            throws Exception {
        plan = List.of(success());
        Files.writeString(directory.resolve("registry"), "existing-file");
        List<Long> waits = new ArrayList<>();

        var exception = assertThrows(
                IOException.class,
                () -> collector(waits).collect(source(), directory)
        );

        assertTrue(exception.getMessage().contains("instantané conservé"));
        assertEquals(1, requests.get());
        assertEquals(List.of(), waits);
        assertEquals(
                "existing-file",
                Files.readString(directory.resolve("registry"))
        );
        assertArchiveCount(1);
        assertNoPartialFiles();
    }

    @Test
    void shouldCountCyclesSeparatelyFromHttpAttemptsAndDuplicates()
            throws Exception {
        plan = List.of(failure(503, null), success(), success());
        List<Long> retryWaits = new ArrayList<>();
        List<Long> cycleWaits = new ArrayList<>();
        List<GtfsRealtimeSnapshotCollector.CollectionResult> reports =
                new ArrayList<>();

        var retryingCollector = collector(retryWaits);
        var loop = new GtfsRealtimeCollectionLoop(
                retryingCollector::collect,
                milliseconds -> cycleWaits.add(milliseconds),
                result -> reports.add(result)
        );

        var summary = loop.run(source(), directory, 30, 2);

        assertEquals(3, requests.get());
        assertEquals(2, summary.completedCycles());
        assertEquals(1, summary.newSnapshots());
        assertEquals(1, summary.duplicateSnapshots());
        assertEquals(2, reports.size());
        assertTrue(reports.get(0).newSnapshot());
        assertFalse(reports.get(1).newSnapshot());
        assertEquals(List.of(5000L), retryWaits);
        assertEquals(List.of(30000L), cycleWaits);
        assertArchiveCount(1);
        assertRegistryEntryCount(1);
        assertNoPartialFiles();
    }

    private GtfsRealtimeRetryingCollector collector(List<Long> waits) {
        return new GtfsRealtimeRetryingCollector(
                new GtfsRealtimeSnapshotCollector()::collect,
                milliseconds -> waits.add(milliseconds),
                CLOCK,
                message -> {
                }
        );
    }

    private URI source() {
        return URI.create(
                "http://127.0.0.1:"
                        + server.getAddress().getPort()
                        + "/feed"
        );
    }

    private static Response failure(int status, String retryAfter) {
        return new Response(
                status,
                "HTTP failure".getBytes(StandardCharsets.UTF_8),
                retryAfter
        );
    }

    private static Response success() {
        byte[] body = FeedMessage.newBuilder()
                .setHeader(
                        FeedHeader.newBuilder()
                                .setGtfsRealtimeVersion("1.0")
                                .setTimestamp(PUBLISHED_AT.getEpochSecond())
                )
                .addEntity(
                        FeedEntity.newBuilder()
                                .setId("entity-test")
                                .setTripUpdate(
                                        TripUpdate.newBuilder()
                                                .setTrip(
                                                        TripDescriptor.newBuilder()
                                                                .setTripId("trip-test")
                                                )
                                )
                )
                .build()
                .toByteArray();

        return new Response(200, body, null);
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

    private void assertRegistryEntryCount(long expected) throws IOException {
        try (var paths = Files.list(directory.resolve("registry"))) {
            long count = paths
                    .filter(path -> path.getFileName()
                            .toString()
                            .endsWith(".json"))
                    .count();

            assertEquals(expected, count);
        }
    }

    private void assertNoPartialFiles() throws IOException {
        try (var paths = Files.walk(directory)) {
            assertFalse(paths.anyMatch(
                    path -> path.getFileName().toString().endsWith(".partial")
            ));
        }
    }
}