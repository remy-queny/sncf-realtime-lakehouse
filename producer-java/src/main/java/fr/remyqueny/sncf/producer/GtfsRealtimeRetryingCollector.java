package fr.remyqueny.sncf.producer;

import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpTimeoutException;
import java.nio.file.Path;
import java.time.Clock;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

public final class GtfsRealtimeRetryingCollector {

    private static final int MAX_ATTEMPTS = 3;
    private static final Duration MAX_RETRY_DELAY = Duration.ofSeconds(300);
    private static final Set<Integer> RETRYABLE_HTTP_STATUSES =
            Set.of(408, 429, 500, 502, 503, 504);

    private final CollectionStep collection;
    private final Sleeper sleeper;
    private final Clock clock;
    private final Consumer<String> reporter;

    public GtfsRealtimeRetryingCollector() {
        this(
                new GtfsRealtimeSnapshotCollector()::collect,
                Thread::sleep,
                Clock.systemUTC(),
                message -> System.err.println(message)
        );
    }

    GtfsRealtimeRetryingCollector(
            CollectionStep collection,
            Sleeper sleeper,
            Clock clock,
            Consumer<String> reporter
    ) {
        this.collection = Objects.requireNonNull(collection);
        this.sleeper = Objects.requireNonNull(sleeper);
        this.clock = Objects.requireNonNull(clock);
        this.reporter = Objects.requireNonNull(reporter);
    }

    @FunctionalInterface
    interface CollectionStep {

        GtfsRealtimeSnapshotCollector.CollectionResult collect(
                URI source,
                Path collectionRoot
        ) throws IOException, InterruptedException;
    }

    @FunctionalInterface
    interface Sleeper {

        void sleep(long milliseconds) throws InterruptedException;
    }

    public GtfsRealtimeSnapshotCollector.CollectionResult collect(
            URI source,
            Path collectionRoot
    ) throws IOException, InterruptedException {

        Objects.requireNonNull(source);
        Objects.requireNonNull(collectionRoot);

        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException(
                        "Interruption demandée avant la tentative de collecte."
                );
            }

            try {
                return collection.collect(source, collectionRoot);
            } catch (IOException exception) {
                Optional<Duration> delay = retryDelay(exception, attempt);

                if (delay.isEmpty()) {
                    throw exception;
                }

                Duration duration = delay.orElseThrow();
                long milliseconds = duration.toMillis();

                if (!duration.minusMillis(milliseconds).isZero()) {
                    milliseconds++;
                }

                reporter.accept(
                        "status=retry"
                                + " failed_attempt=" + attempt
                                + " next_attempt=" + (attempt + 1)
                                + " max_attempts=" + MAX_ATTEMPTS
                                + " delay_ms=" + milliseconds
                                + " exception="
                                + exception.getClass().getSimpleName()
                );

                sleeper.sleep(milliseconds);
            }
        }

        throw new IllegalStateException("Boucle de retry incohérente.");
    }

    private Optional<Duration> retryDelay(
            IOException failure,
            int failedAttempt
    ) {
        if (failedAttempt >= MAX_ATTEMPTS) {
            return Optional.empty();
        }

        Duration backoff = Duration.ofSeconds(
                5L << (failedAttempt - 1)
        );

        if (failure instanceof HttpTimeoutException
                || failure instanceof ConnectException) {
            return Optional.of(backoff);
        }

        if (!(failure instanceof GtfsRealtimeDownloadException download)
                || download.kind()
                != GtfsRealtimeDownloadException.Kind.HTTP_RESPONSE
                || !RETRYABLE_HTTP_STATUSES.contains(download.statusCode())) {
            return Optional.empty();
        }

        if (download.retryAfter().isEmpty()) {
            return Optional.of(backoff);
        }

        Optional<Duration> serverDelay = parseServerDelay(
                download.retryAfter().orElseThrow()
        );

        if (serverDelay.isEmpty()) {
            return Optional.empty();
        }

        Duration delay = serverDelay.orElseThrow().compareTo(backoff) > 0
                ? serverDelay.orElseThrow()
                : backoff;

        if (delay.compareTo(MAX_RETRY_DELAY) > 0) {
            return Optional.empty();
        }

        return Optional.of(delay);
    }

    private Optional<Duration> parseServerDelay(String rawValue) {
        String value = rawValue.strip();

        try {
            Duration delay;

            if (value.matches("[0-9]+")) {
                delay = Duration.ofSeconds(Long.parseLong(value));
            } else {
                var date = ZonedDateTime.parse(
                        value,
                        DateTimeFormatter.RFC_1123_DATE_TIME
                ).toInstant();

                delay = Duration.between(clock.instant(), date);
            }

            return Optional.of(delay.isNegative() ? Duration.ZERO : delay);
        } catch (DateTimeException | ArithmeticException
                 | NumberFormatException exception) {
            return Optional.empty();
        }
    }
}