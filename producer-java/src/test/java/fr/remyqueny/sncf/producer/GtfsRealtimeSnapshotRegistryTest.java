package fr.remyqueny.sncf.producer;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.HexFormat;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GtfsRealtimeSnapshotRegistryTest {

    private static final Instant PUBLISHED_AT =
            Instant.parse("2026-10-28T10:00:00Z");

    @TempDir
    Path directory;

    @Test
    void shouldRegisterNewSnapshot() throws Exception {
        Path registryPath = directory.resolve("registry");
        var registry = new GtfsRealtimeSnapshotRegistry(registryPath);
        var result = snapshot("first.pb", "snapshot-one");

        assertTrue(registry.register(result));

        Path entryPath = registryPath.resolve(result.sha256() + ".json");

        var entry = new ObjectMapper().readValue(
                entryPath.toFile(),
                GtfsRealtimeSnapshotRegistry.SnapshotEntry.class
        );

        assertEquals(result.sha256(), entry.sha256());
        assertEquals(
                result.file().toAbsolutePath().normalize().toString(),
                entry.snapshotPath()
        );
        assertEquals(result.sizeBytes(), entry.sizeBytes());
        assertEquals(PUBLISHED_AT.toString(), entry.publishedAt());
        assertTrue(Files.exists(result.file()));
        assertEntryCount(registryPath, 1);
    }

    @Test
    void shouldIgnoreAlreadyRegisteredContent() throws Exception {
        Path registryPath = directory.resolve("registry");
        var registry = new GtfsRealtimeSnapshotRegistry(registryPath);

        var first = snapshot("first.pb", "same-content");
        var second = snapshot("second.pb", "same-content");

        assertTrue(registry.register(first));

        Path entryPath = registryPath.resolve(first.sha256() + ".json");
        String before = Files.readString(entryPath);

        assertFalse(registry.register(second));

        assertEquals(before, Files.readString(entryPath));
        assertTrue(Files.exists(first.file()));
        assertTrue(Files.exists(second.file()));
        assertEntryCount(registryPath, 1);
    }

    @Test
    void shouldRememberSnapshotAfterRegistryRecreation() throws Exception {
        Path registryPath = directory.resolve("registry");
        var result = snapshot("first.pb", "snapshot-one");

        var firstRegistry = new GtfsRealtimeSnapshotRegistry(registryPath);
        assertTrue(firstRegistry.register(result));

        var restartedRegistry =
                new GtfsRealtimeSnapshotRegistry(registryPath);

        assertFalse(restartedRegistry.register(result));
        assertEntryCount(registryPath, 1);
    }

    @Test
    void shouldRegisterDifferentContentsWithSameTimestamp()
            throws Exception {

        Path registryPath = directory.resolve("registry");
        var registry = new GtfsRealtimeSnapshotRegistry(registryPath);

        var first = snapshot("first.pb", "snapshot-one");
        var second = snapshot("second.pb", "snapshot-two");

        assertEquals(first.publishedAt(), second.publishedAt());
        assertTrue(registry.register(first));
        assertTrue(registry.register(second));
        assertEntryCount(registryPath, 2);
    }

    @Test
    void shouldRejectCorruptedRegistryEntry() throws Exception {
        Path registryPath = directory.resolve("registry");
        var registry = new GtfsRealtimeSnapshotRegistry(registryPath);
        var result = snapshot("first.pb", "snapshot-one");

        Files.createDirectories(registryPath);

        Path entryPath = registryPath.resolve(result.sha256() + ".json");
        Files.writeString(entryPath, "{invalid-json");

        assertThrows(
                IOException.class,
                () -> registry.register(result)
        );

        assertEquals("{invalid-json", Files.readString(entryPath));
    }

    @Test
    void shouldReportRegistryCreationFailure() throws Exception {
        Path registryPath = directory.resolve("not-a-directory");
        Files.writeString(registryPath, "existing-file");

        var registry = new GtfsRealtimeSnapshotRegistry(registryPath);
        var result = snapshot("first.pb", "snapshot-one");

        assertThrows(
                IOException.class,
                () -> registry.register(result)
        );

        assertEquals("existing-file", Files.readString(registryPath));
    }

    @Test
    void shouldRejectDuplicateWhenOriginalArchiveIsMissing()
            throws Exception {

        Path registryPath = directory.resolve("registry");
        var registry = new GtfsRealtimeSnapshotRegistry(registryPath);

        var first = snapshot("first.pb", "same-content");
        var second = snapshot("second.pb", "same-content");

        assertTrue(registry.register(first));
        Files.delete(first.file());

        assertThrows(
                IOException.class,
                () -> registry.register(second)
        );

        assertEntryCount(registryPath, 1);
    }

    private GtfsRealtimeHttpDownloader.DownloadResult snapshot(
            String filename,
            String content
    ) throws Exception {

        byte[] body = content.getBytes(StandardCharsets.UTF_8);
        Path file = directory.resolve(filename);
        Files.write(file, body);

        String sha256 = HexFormat.of().formatHex(
                MessageDigest.getInstance("SHA-256").digest(body)
        );

        return new GtfsRealtimeHttpDownloader.DownloadResult(
                file,
                200,
                body.length,
                "1.0",
                1,
                PUBLISHED_AT,
                sha256
        );
    }

    private void assertEntryCount(Path registryPath, long expected)
            throws IOException {

        try (var files = Files.list(registryPath)) {
            long count = files
                    .filter(path -> path.getFileName()
                            .toString()
                            .endsWith(".json"))
                    .count();

            assertEquals(expected, count);
        }
    }
}
