package fr.remyqueny.sncf.producer;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.Objects;
import java.util.function.Consumer;

public final class GtfsRealtimeCollectionLoop {

    private final CollectionStep collection;
    private final Sleeper sleeper;
    private final Consumer<
            GtfsRealtimeSnapshotCollector.CollectionResult
    > reporter;

    public GtfsRealtimeCollectionLoop() {
        this(
                new GtfsRealtimeSnapshotCollector()::collect,
                Thread::sleep,
                result -> System.out.println(
                        "status="
                                + (result.newSnapshot()
                                ? "collected"
                                : "duplicate")
                                + " sha256=" + result.sha256()
                                + " published_at=" + result.publishedAt()
                                + " archived_file=" + result.archivedFile()
                )
        );
    }

    GtfsRealtimeCollectionLoop(
            CollectionStep collection,
            Sleeper sleeper,
            Consumer<
                    GtfsRealtimeSnapshotCollector.CollectionResult
            > reporter
    ) {
        this.collection = Objects.requireNonNull(collection);
        this.sleeper = Objects.requireNonNull(sleeper);
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

    public record LoopSummary(
            int completedCycles,
            int newSnapshots,
            int duplicateSnapshots
    ) {
    }

    public LoopSummary run(
            URI source,
            Path collectionRoot,
            long intervalSeconds,
            int maxCycles
    ) throws IOException, InterruptedException {

        Objects.requireNonNull(source);
        Objects.requireNonNull(collectionRoot);

        if (intervalSeconds <= 0
                || intervalSeconds > Long.MAX_VALUE / 1000) {
            throw new IllegalArgumentException(
                    "L'intervalle doit être positif et convertible "
                            + "en millisecondes sans dépassement."
            );
        }

        if (maxCycles <= 0) {
            throw new IllegalArgumentException(
                    "Le nombre de cycles doit être strictement positif."
            );
        }

        long intervalMilliseconds = intervalSeconds * 1000;
        int newSnapshots = 0;
        int duplicates = 0;

        for (int cycle = 0; cycle < maxCycles; cycle++) {
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException(
                        "Interruption demandée avant la collecte."
                );
            }

            var result = collection.collect(source, collectionRoot);

            if (result.newSnapshot()) {
                newSnapshots++;
            } else {
                duplicates++;
            }

            reporter.accept(result);

            if (cycle < maxCycles - 1) {
                sleeper.sleep(intervalMilliseconds);
            }
        }

        return new LoopSummary(
                maxCycles,
                newSnapshots,
                duplicates
        );
    }
}