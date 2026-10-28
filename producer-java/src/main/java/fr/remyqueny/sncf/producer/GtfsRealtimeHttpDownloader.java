package fr.remyqueny.sncf.producer;

import com.google.transit.realtime.GtfsRealtime.FeedMessage;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;

public final class GtfsRealtimeHttpDownloader {

    private static final String DEFAULT_URL =
            "https://proxy.transport.data.gouv.fr/resource/"
                    + "sncf-gtfs-rt-trip-updates";

    private static final long MAX_FILE_BYTES = 16L * 1024 * 1024;

    private GtfsRealtimeHttpDownloader() {
    }

    public record DownloadResult(
            Path file,
            int httpStatus,
            long sizeBytes,
            String feedVersion,
            int entityCount,
            Instant publishedAt,
            String sha256
    ) {
    }

    public static DownloadResult download(URI source, Path destination)
            throws IOException, InterruptedException {

        Path output = destination.toAbsolutePath().normalize();

        if (Files.exists(output)) {
            throw new FileAlreadyExistsException(output.toString());
        }

        Files.createDirectories(output.getParent());

        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();

        HttpRequest request = HttpRequest.newBuilder(source)
                .timeout(Duration.ofSeconds(60))
                .header(
                        "Accept",
                        "application/x-protobuf, application/octet-stream"
                )
                .GET()
                .build();

        Path temporary = Files.createTempFile(
                output.getParent(),
                "gtfs-download-",
                ".partial"
        );

        try {
            HttpResponse<Path> response = client.send(
                    request,
                    HttpResponse.BodyHandlers.ofFile(
                            temporary,
                            StandardOpenOption.WRITE,
                            StandardOpenOption.TRUNCATE_EXISTING
                    )
            );

            if (response.statusCode() != 200) {
                throw new IOException(
                        "Téléchargement refusé : HTTP "
                                + response.statusCode()
                );
            }

            long size = Files.size(temporary);

            if (size == 0 || size > MAX_FILE_BYTES) {
                throw new IOException(
                        "Taille du fichier non acceptée : "
                                + size + " octets"
                );
            }

            FeedMessage feed;

            try (InputStream input = Files.newInputStream(temporary)) {
                feed = FeedMessage.parseFrom(input);
            }

            if (!feed.getHeader().hasTimestamp()
                    || feed.getHeader().getTimestamp() <= 0) {
                throw new IOException(
                        "Timestamp de publication absent ou invalide"
                );
            }

            Instant publishedAt = Instant.ofEpochSecond(
                    feed.getHeader().getTimestamp()
            );

            String fingerprint = sha256(temporary);

            Files.move(temporary, output);

            return new DownloadResult(
                    output,
                    response.statusCode(),
                    size,
                    feed.getHeader().getGtfsRealtimeVersion(),
                    feed.getEntityCount(),
                    publishedAt,
                    fingerprint
            );
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    private static String sha256(Path file) throws IOException {
        MessageDigest digest;

        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException(
                    "SHA-256 indisponible dans cet environnement Java.",
                    exception
            );
        }

        try (InputStream input = Files.newInputStream(file)) {
            byte[] buffer = new byte[8192];
            int count;

            while ((count = input.read(buffer)) != -1) {
                digest.update(buffer, 0, count);
            }
        }

        return HexFormat.of().formatHex(digest.digest());
    }

    public static void main(String[] args)
            throws IOException, InterruptedException {

        if (args.length != 1) {
            throw new IllegalArgumentException(
                    "Usage : GtfsRealtimeHttpDownloader nouveau_fichier.pb"
            );
        }

        String configuredUrl = System.getenv("SNCF_GTFS_RT_URL");

        String sourceUrl = configuredUrl == null || configuredUrl.isBlank()
                ? DEFAULT_URL
                : configuredUrl;

        DownloadResult result = download(
                URI.create(sourceUrl),
                Path.of(args[0])
        );

        System.out.println("HTTP : " + result.httpStatus());
        System.out.println("Octets téléchargés : " + result.sizeBytes());
        System.out.println("Version GTFS-RT : " + result.feedVersion());
        System.out.println("Entités : " + result.entityCount());
        System.out.println(
                "Timestamp de publication UTC : " + result.publishedAt()
        );
        System.out.println("SHA-256 : " + result.sha256());
        System.out.println("Fichier : " + result.file());
        System.out.println("Téléchargement validé, aucun envoi Kafka.");
    }
}