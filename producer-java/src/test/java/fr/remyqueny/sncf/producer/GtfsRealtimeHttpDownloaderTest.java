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
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GtfsRealtimeHttpDownloaderTest {

    private static final Instant PUBLISHED_AT =
            Instant.parse("2026-10-15T16:54:10Z");

    @TempDir
    Path directory;

    private HttpServer server;

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(
                new InetSocketAddress("127.0.0.1", 0),
                0
        );
        server.start();
    }

    @AfterEach
    void stopServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void shouldDownloadAndValidateFeed() throws Exception {
        byte[] body = feed(true);
        respond(200, body);

        Path destination = directory.resolve("trip_updates.pb");

        var result = GtfsRealtimeHttpDownloader.download(
                source(),
                destination
        );

        assertEquals(200, result.httpStatus());
        assertEquals((long) body.length, result.sizeBytes());
        assertEquals("1.0", result.feedVersion());
        assertEquals(1, result.entityCount());
        assertEquals(PUBLISHED_AT, result.publishedAt());
        assertEquals(
                destination.toAbsolutePath().normalize(),
                result.file()
        );

        assertArrayEquals(body, Files.readAllBytes(destination));
        assertFileCount(1);
    }

    @Test
    void shouldRejectHttpError() throws Exception {
        respond(
                503,
                "Service unavailable".getBytes(StandardCharsets.UTF_8)
        );

        IOException exception = assertThrows(
                IOException.class,
                () -> GtfsRealtimeHttpDownloader.download(
                        source(),
                        directory.resolve("trip_updates.pb")
                )
        );

        assertTrue(exception.getMessage().contains("503"));
        assertFileCount(0);
    }

    @Test
    void shouldRejectEmptyResponse() throws Exception {
        respond(200, new byte[0]);

        assertThrows(
                IOException.class,
                () -> GtfsRealtimeHttpDownloader.download(
                        source(),
                        directory.resolve("trip_updates.pb")
                )
        );

        assertFileCount(0);
    }

    @Test
    void shouldRejectInvalidProtobuf() throws Exception {
        respond(200, new byte[]{0});

        assertThrows(
                IOException.class,
                () -> GtfsRealtimeHttpDownloader.download(
                        source(),
                        directory.resolve("trip_updates.pb")
                )
        );

        assertFileCount(0);
    }

    @Test
    void shouldRejectMissingTimestamp() throws Exception {
        respond(200, feed(false));

        IOException exception = assertThrows(
                IOException.class,
                () -> GtfsRealtimeHttpDownloader.download(
                        source(),
                        directory.resolve("trip_updates.pb")
                )
        );

        assertTrue(exception.getMessage().contains("Timestamp"));
        assertFileCount(0);
    }

    @Test
    void shouldNotOverwriteExistingFile() throws Exception {
        Path destination = directory.resolve("trip_updates.pb");
        byte[] original = "existing-file".getBytes(StandardCharsets.UTF_8);

        Files.write(destination, original);

        assertThrows(
                FileAlreadyExistsException.class,
                () -> GtfsRealtimeHttpDownloader.download(
                        source(),
                        destination
                )
        );

        assertArrayEquals(original, Files.readAllBytes(destination));
        assertFileCount(1);
    }

    private URI source() {
        return URI.create(
                "http://127.0.0.1:"
                        + server.getAddress().getPort()
                        + "/feed"
        );
    }

    private void respond(int status, byte[] body) {
        server.createContext("/feed", exchange -> {
            try {
                exchange.getResponseHeaders().set(
                        "Content-Type",
                        "application/x-protobuf"
                );

                exchange.sendResponseHeaders(status, body.length);

                try (OutputStream output = exchange.getResponseBody()) {
                    output.write(body);
                }
            } finally {
                exchange.close();
            }
        });
    }

    private static byte[] feed(boolean includeTimestamp) {
        FeedHeader.Builder header = FeedHeader.newBuilder()
                .setGtfsRealtimeVersion("1.0");

        if (includeTimestamp) {
            header.setTimestamp(PUBLISHED_AT.getEpochSecond());
        }

        return FeedMessage.newBuilder()
                .setHeader(header)
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
    }

    private void assertFileCount(long expected) throws IOException {
        try (var files = Files.list(directory)) {
            assertEquals(expected, files.count());
        }
    }
}