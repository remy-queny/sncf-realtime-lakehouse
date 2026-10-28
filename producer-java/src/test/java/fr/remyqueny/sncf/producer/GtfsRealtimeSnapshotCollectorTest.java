package fr.remyqueny.sncf.producer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GtfsRealtimeSnapshotCollectorTest {

    private static final URI SOURCE =
            URI.create("https://example.invalid/feed");

    private static final Instant PUBLISHED_AT =
            Instant.parse("2026-10-28T10:00:00Z");

    @TempDir
    Path directory;

    @Test
    void shouldCollectAndRegisterNewSnapshot() throws Exception {
        var collector = collectorFor("snapshot-one");

        var result = collector.collect(SOURCE, directory);

        assertTrue(result.newSnapshot());
        assertEquals(PUBLISHED_AT, result.publishedAt());
        assertTrue(Files.isRegularFile(result.archivedFile()));

        assertEquals(
                "snapshot-one",
                Files.readString(result.archivedFile())
        );

        assertTrue(Files.isRegularFile(
                directory.resolve("registry")
                        .resolve(result.sha256() + ".json")
        ));

        assertSnapshotCount(1);
    }

    @Test
    void shouldRemoveDuplicateCopyAfterCollectorRecreation()
            throws Exception {

        var firstCollector = collectorFor("same-content");
        var first = firstCollector.collect(SOURCE, directory);

        var restartedCollector = collectorFor("same-content");
        var second = restartedCollector.collect(SOURCE, directory);

        assertTrue(first.newSnapshot());
        assertFalse(second.newSnapshot());
        assertEquals(first.sha256(), second.sha256());
        assertNull(second.archivedFile());

        assertTrue(Files.isRegularFile(first.archivedFile()));
        assertSnapshotCount(1);
    }

    @Test
    void shouldKeepDifferentContentsWithSameTimestamp()
            throws Exception {

        var first = collectorFor("snapshot-one")
                .collect(SOURCE, directory);

        var second = collectorFor("snapshot-two")
                .collect(SOURCE, directory);

        assertTrue(first.newSnapshot());
        assertTrue(second.newSnapshot());
        assertEquals(first.publishedAt(), second.publishedAt());
        assertNotEquals(first.sha256(), second.sha256());

        assertTrue(Files.isRegularFile(first.archivedFile()));
        assertTrue(Files.isRegularFile(second.archivedFile()));
        assertSnapshotCount(2);
    }

    @Test
    void shouldNotRegisterFailedDownload() throws Exception {
        var collector = new GtfsRealtimeSnapshotCollector(
                (source, destination) -> {
                    throw new IOException("HTTP 503 simulé");
                }
        );

        IOException exception = assertThrows(
                IOException.class,
                () -> collector.collect(SOURCE, directory)
        );

        assertTrue(exception.getMessage().contains("503"));
        assertFalse(Files.exists(directory.resolve("registry")));
        assertSnapshotCount(0);
    }

    @Test
    void shouldKeepDownloadedFileWhenRegistrationFails()
            throws Exception {

        Path blockedRegistry = directory.resolve("registry");
        Files.writeString(blockedRegistry, "existing-file");

        var collector = collectorFor("snapshot-one");

        IOException exception = assertThrows(
                IOException.class,
                () -> collector.collect(SOURCE, directory)
        );

        assertTrue(
                exception.getMessage().contains("instantané conservé")
        );

        assertEquals(
                "existing-file",
                Files.readString(blockedRegistry)
        );

        assertSnapshotCount(1);
    }

    private GtfsRealtimeSnapshotCollector collectorFor(String content) {
        return new GtfsRealtimeSnapshotCollector(
                (source, destination) -> {
                    byte[] body = content.getBytes(
                            StandardCharsets.UTF_8
                    );

                    Files.write(destination, body);

                    return new GtfsRealtimeHttpDownloader.DownloadResult(
                            destination,
                            200,
                            body.length,
                            "1.0",
                            1,
                            PUBLISHED_AT,
                            sha256(body)
                    );
                }
        );
    }

    private static String sha256(byte[] body) {
        try {
            return HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(body)
            );
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException(exception);
        }
    }

    private void assertSnapshotCount(long expected) throws IOException {
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
}