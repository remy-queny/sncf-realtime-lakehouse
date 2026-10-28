package fr.remyqueny.sncf.producer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GtfsRealtimeCollectionLoopTest {

    private static final URI SOURCE =
            URI.create("https://example.invalid/feed");

    @TempDir
    Path directory;

    @Test
    void shouldRunSequentialCyclesAndCountDuplicates() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        List<String> events = new ArrayList<>();

        var loop = new GtfsRealtimeCollectionLoop(
                (source, root) -> {
                    assertEquals(SOURCE, source);
                    assertEquals(directory, root);

                    int call = calls.incrementAndGet();
                    events.add("collect-" + call);

                    return result(call == 1);
                },
                milliseconds -> events.add("wait-" + milliseconds),
                snapshot -> events.add(
                        snapshot.newSnapshot()
                                ? "report-new"
                                : "report-duplicate"
                )
        );

        var summary = loop.run(SOURCE, directory, 30, 3);

        assertEquals(3, summary.completedCycles());
        assertEquals(1, summary.newSnapshots());
        assertEquals(2, summary.duplicateSnapshots());

        assertEquals(
                List.of(
                        "collect-1",
                        "report-new",
                        "wait-30000",
                        "collect-2",
                        "report-duplicate",
                        "wait-30000",
                        "collect-3",
                        "report-duplicate"
                ),
                events
        );
    }

    @Test
    void shouldNotWaitAfterLastCycle() throws Exception {
        List<Long> waits = new ArrayList<>();

        var loop = new GtfsRealtimeCollectionLoop(
                (source, root) -> result(true),
                milliseconds -> waits.add(milliseconds),
                snapshot -> {
                }
        );

        var summary = loop.run(SOURCE, directory, 30, 1);

        assertEquals(1, summary.completedCycles());
        assertEquals(1, summary.newSnapshots());
        assertEquals(0, summary.duplicateSnapshots());
        assertEquals(List.of(), waits);
    }

    @Test
    void shouldStopImmediatelyAfterCollectionFailure() {
        AtomicInteger calls = new AtomicInteger();
        AtomicInteger reports = new AtomicInteger();
        List<Long> waits = new ArrayList<>();

        var loop = new GtfsRealtimeCollectionLoop(
                (source, root) -> {
                    if (calls.incrementAndGet() == 2) {
                        throw new IOException("Échec simulé");
                    }

                    return result(true);
                },
                milliseconds -> waits.add(milliseconds),
                snapshot -> reports.incrementAndGet()
        );

        IOException exception = assertThrows(
                IOException.class,
                () -> loop.run(SOURCE, directory, 30, 5)
        );

        assertEquals("Échec simulé", exception.getMessage());
        assertEquals(2, calls.get());
        assertEquals(1, reports.get());
        assertEquals(List.of(30000L), waits);
    }

    @Test
    void shouldStopWhenWaitingIsInterrupted() {
        AtomicInteger calls = new AtomicInteger();
        AtomicInteger reports = new AtomicInteger();

        var loop = new GtfsRealtimeCollectionLoop(
                (source, root) -> {
                    calls.incrementAndGet();
                    return result(true);
                },
                milliseconds -> {
                    throw new InterruptedException("Arrêt simulé");
                },
                snapshot -> reports.incrementAndGet()
        );

        assertThrows(
                InterruptedException.class,
                () -> loop.run(SOURCE, directory, 30, 5)
        );

        assertEquals(1, calls.get());
        assertEquals(1, reports.get());
    }

    @Test
    void shouldRejectInvalidIntervalsBeforeCollection() {
        AtomicInteger calls = new AtomicInteger();

        var loop = new GtfsRealtimeCollectionLoop(
                (source, root) -> {
                    calls.incrementAndGet();
                    return result(true);
                },
                milliseconds -> {
                },
                snapshot -> {
                }
        );

        for (long interval : new long[]{0, -1, Long.MAX_VALUE}) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> loop.run(SOURCE, directory, interval, 3)
            );
        }

        assertEquals(0, calls.get());
    }

    @Test
    void shouldRejectInvalidCycleCountsBeforeCollection() {
        AtomicInteger calls = new AtomicInteger();

        var loop = new GtfsRealtimeCollectionLoop(
                (source, root) -> {
                    calls.incrementAndGet();
                    return result(true);
                },
                milliseconds -> {
                },
                snapshot -> {
                }
        );

        for (int cycles : new int[]{0, -1}) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> loop.run(SOURCE, directory, 30, cycles)
            );
        }

        assertEquals(0, calls.get());
    }

    private GtfsRealtimeSnapshotCollector.CollectionResult result(
            boolean isNew
    ) {
        return new GtfsRealtimeSnapshotCollector.CollectionResult(
                isNew,
                "a".repeat(64),
                Instant.parse("2026-10-28T10:00:00Z"),
                isNew ? directory.resolve("tripupdates.pb") : null
        );
    }
}