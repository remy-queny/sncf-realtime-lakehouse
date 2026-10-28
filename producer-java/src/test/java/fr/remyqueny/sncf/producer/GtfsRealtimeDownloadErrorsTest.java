package fr.remyqueny.sncf.producer;

import com.google.transit.realtime.GtfsRealtime.FeedHeader;
import com.google.transit.realtime.GtfsRealtime.FeedMessage;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GtfsRealtimeDownloadErrorsTest {

    @TempDir
    Path directory;

    private HttpServer server;
    private final AtomicInteger requests = new AtomicInteger();

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
    void shouldExposeHttpStatusAndRetryAfter() throws Exception {
        respond(503, "Unavailable".getBytes(StandardCharsets.UTF_8), "120");

        var exception = assertThrows(
                GtfsRealtimeDownloadException.class,
                () -> GtfsRealtimeHttpDownloader.download(
                        source(), directory.resolve("snapshot.pb")
                )
        );

        assertEquals(
                GtfsRealtimeDownloadException.Kind.HTTP_RESPONSE,
                exception.kind()
        );
        assertEquals(503, exception.statusCode());
        assertEquals("120", exception.retryAfter().orElseThrow());
        assertDirectoryEmpty();
    }

    @Test
    void shouldExposeNonTransientHttpResponseWithoutRetryAfter()
            throws Exception {
        respond(401, "Unauthorized".getBytes(StandardCharsets.UTF_8), null);

        var exception = assertThrows(
                GtfsRealtimeDownloadException.class,
                () -> GtfsRealtimeHttpDownloader.download(
                        source(), directory.resolve("snapshot.pb")
                )
        );

        assertEquals(
                GtfsRealtimeDownloadException.Kind.HTTP_RESPONSE,
                exception.kind()
        );
        assertEquals(401, exception.statusCode());
        assertTrue(exception.retryAfter().isEmpty());
        assertDirectoryEmpty();
    }

    @Test
    void shouldClassifyMalformedProtobufAsInvalidFeed() throws Exception {
        respond(200, new byte[]{0}, null);

        var exception = assertThrows(
                GtfsRealtimeDownloadException.class,
                () -> GtfsRealtimeHttpDownloader.download(
                        source(), directory.resolve("snapshot.pb")
                )
        );

        assertEquals(
                GtfsRealtimeDownloadException.Kind.INVALID_FEED,
                exception.kind()
        );
        assertEquals(0, exception.statusCode());
        assertTrue(exception.retryAfter().isEmpty());
        assertNotNull(exception.getCause());
        assertDirectoryEmpty();
    }

    @Test
    void shouldClassifyMissingTimestampAsInvalidFeed() throws Exception {
        byte[] body = FeedMessage.newBuilder()
                .setHeader(
                        FeedHeader.newBuilder().setGtfsRealtimeVersion("1.0")
                )
                .build()
                .toByteArray();

        respond(200, body, null);

        var exception = assertThrows(
                GtfsRealtimeDownloadException.class,
                () -> GtfsRealtimeHttpDownloader.download(
                        source(), directory.resolve("snapshot.pb")
                )
        );

        assertEquals(
                GtfsRealtimeDownloadException.Kind.INVALID_FEED,
                exception.kind()
        );
        assertTrue(exception.getMessage().contains("Timestamp"));
        assertDirectoryEmpty();
    }

    @Test
    void shouldClassifyOutOfRangeTimestampAsInvalidFeed() throws Exception {
        byte[] body = FeedMessage.newBuilder()
                .setHeader(
                        FeedHeader.newBuilder()
                                .setGtfsRealtimeVersion("1.0")
                                .setTimestamp(Long.MAX_VALUE)
                )
                .build()
                .toByteArray();

        respond(200, body, null);

        var exception = assertThrows(
                GtfsRealtimeDownloadException.class,
                () -> GtfsRealtimeHttpDownloader.download(
                        source(), directory.resolve("snapshot.pb")
                )
        );

        assertEquals(
                GtfsRealtimeDownloadException.Kind.INVALID_FEED,
                exception.kind()
        );
        assertNotNull(exception.getCause());
        assertDirectoryEmpty();
    }

    @Test
    void shouldKeepLocalFileErrorDistinctWithoutHttpRequest()
            throws Exception {
        respond(503, "Unavailable".getBytes(StandardCharsets.UTF_8), null);

        Path destination = directory.resolve("snapshot.pb");
        Files.writeString(destination, "existing-file");

        assertThrows(
                FileAlreadyExistsException.class,
                () -> GtfsRealtimeHttpDownloader.download(source(), destination)
        );

        assertEquals(0, requests.get());
        assertEquals("existing-file", Files.readString(destination));
    }

    private URI source() {
        return URI.create(
                "http://127.0.0.1:"
                        + server.getAddress().getPort()
                        + "/feed"
        );
    }

    private void respond(int status, byte[] body, String retryAfter) {
        server.createContext("/feed", exchange -> {
            requests.incrementAndGet();

            try {
                exchange.getResponseHeaders().set(
                        "Content-Type", "application/x-protobuf"
                );

                if (retryAfter != null) {
                    exchange.getResponseHeaders().set("Retry-After", retryAfter);
                }

                exchange.sendResponseHeaders(status, body.length);

                try (OutputStream output = exchange.getResponseBody()) {
                    output.write(body);
                }
            } finally {
                exchange.close();
            }
        });
    }

    private void assertDirectoryEmpty() throws IOException {
        try (var paths = Files.list(directory)) {
            assertEquals(0L, paths.count());
        }
    }
}