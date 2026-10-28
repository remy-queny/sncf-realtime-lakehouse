package fr.remyqueny.sncf.producer;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Instant;

public final class GtfsRealtimeSnapshotRegistry {

    private final Path directory;
    private final ObjectMapper mapper = new ObjectMapper();

    public GtfsRealtimeSnapshotRegistry(Path directory) {
        this.directory = directory.toAbsolutePath().normalize();
    }

    public record SnapshotEntry(
            String sha256,
            String snapshotPath,
            long sizeBytes,
            String feedVersion,
            int entityCount,
            String publishedAt,
            String collectedAt
    ) {
    }

    public boolean register(
            GtfsRealtimeHttpDownloader.DownloadResult result
    ) throws IOException {

        validateResult(result);
        Files.createDirectories(directory);

        Path lockPath = directory.resolve(".registry.lock");

        try (
                FileChannel channel = FileChannel.open(
                        lockPath,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.WRITE
                );
                FileLock lock = channel.lock()
        ) {
            return registerWhileLocked(result);
        } catch (OverlappingFileLockException exception) {
            throw new IOException(
                    "Le registre est déjà utilisé dans cette JVM.",
                    exception
            );
        }
    }

    private boolean registerWhileLocked(
            GtfsRealtimeHttpDownloader.DownloadResult result
    ) throws IOException {

        Path entryPath = directory.resolve(result.sha256() + ".json");

        if (Files.exists(entryPath)) {
            readAndValidateEntry(entryPath, result.sha256());
            return false;
        }

        SnapshotEntry entry = new SnapshotEntry(
                result.sha256(),
                result.file().toAbsolutePath().normalize().toString(),
                result.sizeBytes(),
                result.feedVersion(),
                result.entityCount(),
                result.publishedAt().toString(),
                Instant.now().toString()
        );

        Path temporary = Files.createTempFile(
                directory,
                ".snapshot-",
                ".partial"
        );

        try {
            mapper.writerWithDefaultPrettyPrinter()
                    .writeValue(temporary.toFile(), entry);

            Files.move(
                    temporary,
                    entryPath,
                    StandardCopyOption.ATOMIC_MOVE
            );
        } finally {
            Files.deleteIfExists(temporary);
        }

        return true;
    }

    private void readAndValidateEntry(
            Path entryPath,
            String expectedSha256
    ) throws IOException {

        SnapshotEntry entry = mapper.readValue(
                entryPath.toFile(),
                SnapshotEntry.class
        );

        if (entry == null
                || !expectedSha256.equals(entry.sha256())
                || entry.snapshotPath() == null
                || entry.snapshotPath().isBlank()
                || entry.sizeBytes() <= 0
                || entry.feedVersion() == null
                || entry.feedVersion().isBlank()
                || entry.entityCount() < 0) {
            throw new IOException(
                    "Entrée de registre invalide : " + entryPath
            );
        }

        Path snapshot;

        try {
            snapshot = Path.of(entry.snapshotPath());

            if (!snapshot.isAbsolute()
                    || Instant.parse(entry.publishedAt()).getEpochSecond() <= 0) {
                throw new IllegalArgumentException(
                        "Chemin ou timestamp de publication invalide."
                );
            }

            Instant.parse(entry.collectedAt());
        } catch (RuntimeException exception) {
            throw new IOException(
                    "Métadonnées de registre invalides : " + entryPath,
                    exception
            );
        }

        validateSnapshotFile(snapshot, entry.sizeBytes());
    }

    private static void validateResult(
            GtfsRealtimeHttpDownloader.DownloadResult result
    ) throws IOException {

        if (result == null
                || result.sha256() == null
                || !result.sha256().matches("[0-9a-f]{64}")
                || result.file() == null
                || result.httpStatus() != 200
                || result.sizeBytes() <= 0
                || result.feedVersion() == null
                || result.feedVersion().isBlank()
                || result.entityCount() < 0
                || result.publishedAt() == null
                || result.publishedAt().getEpochSecond() <= 0) {
            throw new IllegalArgumentException(
                    "Résultat de téléchargement invalide."
            );
        }

        validateSnapshotFile(result.file(), result.sizeBytes());
    }

    private static void validateSnapshotFile(
            Path snapshot,
            long expectedSize
    ) throws IOException {

        if (!Files.isRegularFile(snapshot)
                || Files.size(snapshot) != expectedSize) {
            throw new IOException(
                    "Instantané absent ou taille incohérente : " + snapshot
            );
        }
    }
}