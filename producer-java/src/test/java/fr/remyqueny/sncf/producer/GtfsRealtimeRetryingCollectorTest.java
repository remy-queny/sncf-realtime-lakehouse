package fr.remyqueny.sncf.producer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpTimeoutException;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GtfsRealtimeRetryingCollectorTest {

    private static final URI SOURCE =
            URI.create("https://example.invalid/feed");
    private static final Instant NOW =
            Instant.parse("2026-10-28T10:00:00Z");
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);

    @TempDir
    Path directory;

    @Test
    void shouldReturnImmediatelyAfterSuccess() throws Exception {
        List<Long> waits = new ArrayList<>();
        AtomicInteger calls = new AtomicInteger();
        var expected = result();

        var collector = collector((source, root) -> {
            calls.incrementAndGet();
            return expected;
        }, waits);

        assertSame(expected, collector.collect(SOURCE, directory));
        assertEquals(1, calls.get());
        assertEquals(List.of(), waits);
    }

    @Test
    void shouldRetrySelectedHttpStatusesThenSucceed() throws Exception {
        for (int status : new int[]{408, 429, 500, 502, 503, 504}) {
            List<Long> waits = new ArrayList<>();
            AtomicInteger calls = new AtomicInteger();

            var collector = collector((source, root) -> {
                if (calls.incrementAndGet() == 1) {
                    throw GtfsRealtimeDownloadException.httpResponse(status, null);
                }
                return result();
            }, waits);

            collector.collect(SOURCE, directory);

            assertEquals(2, calls.get());
            assertEquals(List.of(5000L), waits);
        }
    }

    @Test
    void shouldStopAfterThreeAttempts() {
        List<Long> waits = new ArrayList<>();
        AtomicInteger calls = new AtomicInteger();
        var failure = GtfsRealtimeDownloadException.httpResponse(503, null);

        var collector = collector((source, root) -> {
            calls.incrementAndGet();
            throw failure;
        }, waits);

        var actual = assertThrows(
                GtfsRealtimeDownloadException.class,
                () -> collector.collect(SOURCE, directory)
        );

        assertSame(failure, actual);
        assertEquals(3, calls.get());
        assertEquals(List.of(5000L, 10000L), waits);
    }

    @Test
    void shouldNotRetryOtherHttpStatuses() {
        for (int status : new int[]{400, 401, 403, 404, 501}) {
            List<Long> waits = new ArrayList<>();
            AtomicInteger calls = new AtomicInteger();
            var failure = GtfsRealtimeDownloadException.httpResponse(status, null);

            var collector = collector((source, root) -> {
                calls.incrementAndGet();
                throw failure;
            }, waits);

            assertSame(failure, assertThrows(
                    GtfsRealtimeDownloadException.class,
                    () -> collector.collect(SOURCE, directory)
            ));

            assertEquals(1, calls.get());
            assertEquals(List.of(), waits);
        }
    }

    @Test
    void shouldHonorRetryAfterSeconds() throws Exception {
        List<Long> waits = new ArrayList<>();
        AtomicInteger calls = new AtomicInteger();

        var collector = collector((source, root) -> {
            if (calls.incrementAndGet() == 1) {
                throw GtfsRealtimeDownloadException.httpResponse(429, "120");
            }
            return result();
        }, waits);

        collector.collect(SOURCE, directory);
        assertEquals(List.of(120000L), waits);
    }

    @Test
    void shouldHonorRetryAfterHttpDate() throws Exception {
        List<Long> waits = new ArrayList<>();
        AtomicInteger calls = new AtomicInteger();
        String retryAfter = DateTimeFormatter.RFC_1123_DATE_TIME.format(
                NOW.plusSeconds(90).atZone(ZoneOffset.UTC)
        );

        var collector = collector((source, root) -> {
            if (calls.incrementAndGet() == 1) {
                throw GtfsRealtimeDownloadException.httpResponse(503, retryAfter);
            }
            return result();
        }, waits);

        collector.collect(SOURCE, directory);
        assertEquals(List.of(90000L), waits);
    }

    @Test
    void shouldKeepBackoffWhenServerRequestsShorterWait() throws Exception {
        List<Long> waits = new ArrayList<>();
        AtomicInteger calls = new AtomicInteger();

        var collector = collector((source, root) -> {
            if (calls.incrementAndGet() == 1) {
                throw GtfsRealtimeDownloadException.httpResponse(503, "1");
            }
            return result();
        }, waits);

        collector.collect(SOURCE, directory);
        assertEquals(List.of(5000L), waits);
    }

    @Test
    void shouldStopInsteadOfShorteningExcessiveServerWait() {
        List<Long> waits = new ArrayList<>();
        AtomicInteger calls = new AtomicInteger();
        var failure = GtfsRealtimeDownloadException.httpResponse(503, "301");

        var collector = collector((source, root) -> {
            calls.incrementAndGet();
            throw failure;
        }, waits);

        assertSame(failure, assertThrows(
                GtfsRealtimeDownloadException.class,
                () -> collector.collect(SOURCE, directory)
        ));
        assertEquals(1, calls.get());
        assertEquals(List.of(), waits);
    }

    @Test
    void shouldStopForInvalidRetryAfter() {
        for (String header : new String[]{"invalid", "-1", "", "999999999999999999999"}) {
            List<Long> waits = new ArrayList<>();
            AtomicInteger calls = new AtomicInteger();
            var failure = GtfsRealtimeDownloadException.httpResponse(503, header);

            var collector = collector((source, root) -> {
                calls.incrementAndGet();
                throw failure;
            }, waits);

            assertSame(failure, assertThrows(
                    GtfsRealtimeDownloadException.class,
                    () -> collector.collect(SOURCE, directory)
            ));
            assertEquals(1, calls.get());
            assertEquals(List.of(), waits);
        }
    }

    @Test
    void shouldRetryExplicitNetworkFailures() throws Exception {
        for (IOException failure : List.of(
                new HttpTimeoutException("Timeout simulé"),
                new ConnectException("Connexion refusée simulée")
        )) {
            List<Long> waits = new ArrayList<>();
            AtomicInteger calls = new AtomicInteger();

            var collector = collector((source, root) -> {
                if (calls.incrementAndGet() == 1) {
                    throw failure;
                }
                return result();
            }, waits);

            collector.collect(SOURCE, directory);
            assertEquals(2, calls.get());
            assertEquals(List.of(5000L), waits);
        }
    }

    @Test
    void shouldNotRetryInvalidFeed() {
        List<Long> waits = new ArrayList<>();
        AtomicInteger calls = new AtomicInteger();
        var failure = GtfsRealtimeDownloadException.invalidFeed("Flux invalide");

        var collector = collector((source, root) -> {
            calls.incrementAndGet();
            throw failure;
        }, waits);

        assertSame(failure, assertThrows(
                GtfsRealtimeDownloadException.class,
                () -> collector.collect(SOURCE, directory)
        ));
        assertEquals(1, calls.get());
        assertEquals(List.of(), waits);
    }

    @Test
    void shouldNotRetryGenericIoFailureEvenWithNetworkCause() {
        List<Long> waits = new ArrayList<>();
        AtomicInteger calls = new AtomicInteger();
        var failure = new IOException(
                "Échec du registre",
                new HttpTimeoutException("Cause simulée")
        );

        var collector = collector((source, root) -> {
            calls.incrementAndGet();
            throw failure;
        }, waits);

        assertSame(failure, assertThrows(
                IOException.class,
                () -> collector.collect(SOURCE, directory)
        ));
        assertEquals(1, calls.get());
        assertEquals(List.of(), waits);
    }

    @Test
    void shouldPropagateInterruptionDuringBackoff() {
        AtomicInteger calls = new AtomicInteger();
        var collector = new GtfsRealtimeRetryingCollector(
                (source, root) -> {
                    calls.incrementAndGet();
                    throw GtfsRealtimeDownloadException.httpResponse(503, null);
                },
                milliseconds -> {
                    throw new InterruptedException("Arrêt simulé");
                },
                CLOCK,
                message -> {
                }
        );

        assertThrows(
                InterruptedException.class,
                () -> collector.collect(SOURCE, directory)
        );
        assertEquals(1, calls.get());
    }

    @Test
    void shouldNotCollectWhenThreadIsAlreadyInterrupted() {
        AtomicInteger calls = new AtomicInteger();
        var collector = collector((source, root) -> {
            calls.incrementAndGet();
            return result();
        }, new ArrayList<>());

        try {
            Thread.currentThread().interrupt();
            assertThrows(
                    InterruptedException.class,
                    () -> collector.collect(SOURCE, directory)
            );
            assertEquals(0, calls.get());
        } finally {
            Thread.interrupted();
        }
    }

    private GtfsRealtimeRetryingCollector collector(
            GtfsRealtimeRetryingCollector.CollectionStep step,
            List<Long> waits
    ) {
        return new GtfsRealtimeRetryingCollector(
                step,
                milliseconds -> waits.add(milliseconds),
                CLOCK,
                message -> {
                }
        );
    }

    private GtfsRealtimeSnapshotCollector.CollectionResult result() {
        return new GtfsRealtimeSnapshotCollector.CollectionResult(
                true,
                "a".repeat(64),
                NOW,
                directory.resolve("snapshot.pb")
        );
    }
}