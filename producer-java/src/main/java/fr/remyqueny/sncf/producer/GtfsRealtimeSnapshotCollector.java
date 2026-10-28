package fr.remyqueny.sncf.producer;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Objects;

public final class GtfsRealtimeSnapshotCollector {

    private final Downloader downloader;

    public GtfsRealtimeSnapshotCollector() {
        this(GtfsRealtimeHttpDownloader::download);
    }

    GtfsRealtimeSnapshotCollector(Downloader downloader) {
        this.downloader = Objects.requireNonNull(downloader);
    }

    @FunctionalInterface
    interface Downloader {

        GtfsRealtimeHttpDownloader.DownloadResult download(
                URI source,
                Path destination
        ) throws IOException, InterruptedException;
    }

    public record CollectionResult(
            boolean newSnapshot,
            String sha256,
            Instant publishedAt,
            Path archivedFile
    ) {
    }

    public CollectionResult collect(URI source, Path collectionRoot)
            throws IOException, InterruptedException {

        Objects.requireNonNull(source);
        Objects.requireNonNull(collectionRoot);

        Path root = collectionRoot.toAbsolutePath().normalize();
        Path snapshots = root.resolve("snapshots");

        Files.createDirectories(snapshots);

        Path attemptDirectory = Files.createTempDirectory(
                snapshots,
                "snapshot-"
        );

        Path candidate = attemptDirectory.resolve("tripupdates.pb");

        GtfsRealtimeHttpDownloader.DownloadResult downloaded =
                downloader.download(source, candidate);

        var registry = new GtfsRealtimeSnapshotRegistry(
                root.resolve("registry")
        );

        boolean isNew;

        try {
            isNew = registry.register(downloaded);
        } catch (IOException exception) {
            throw new IOException(
                    "Échec de l'enregistrement ; instantané conservé : "
                            + candidate,
                    exception
            );
        }

        if (!isNew) {
            Files.delete(candidate);
            Files.delete(attemptDirectory);
        }

        return new CollectionResult(
                isNew,
                downloaded.sha256(),
                downloaded.publishedAt(),
                isNew ? candidate : null
        );
    }
}