package fr.remyqueny.sncf.producer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GtfsRealtimeCollectorApplicationTest {

    @TempDir
    Path directory;

    private final ByteArrayOutputStream stdout =
            new ByteArrayOutputStream();

    private final ByteArrayOutputStream stderr =
            new ByteArrayOutputStream();

    @Test
    void shouldShowHelpWithoutCollecting() {
        AtomicInteger calls = new AtomicInteger();

        int code = execute(
                new String[]{"--help"},
                (source, root, interval, cycles) -> {
                    calls.incrementAndGet();
                    return new GtfsRealtimeCollectionLoop.LoopSummary(
                            1, 1, 0
                    );
                }
        );

        assertEquals(0, code);
        assertEquals(0, calls.get());
        assertTrue(output().contains("Usage"));
        assertTrue(output().contains("aucun export JSONL"));
        assertEquals("", errors());
    }

    @Test
    void shouldPassValidatedOptionsAndReportSummary() {
        AtomicInteger calls = new AtomicInteger();

        int code = execute(
                validArguments(),
                (source, root, interval, cycles) -> {
                    calls.incrementAndGet();

                    assertEquals(
                            URI.create("https://example.invalid/feed"),
                            source
                    );

                    assertEquals(
                            directory.toAbsolutePath().normalize(),
                            root
                    );

                    assertEquals(30L, interval);
                    assertEquals(3, cycles);

                    return new GtfsRealtimeCollectionLoop.LoopSummary(
                            3, 1, 2
                    );
                }
        );

        assertEquals(0, code);
        assertEquals(1, calls.get());

        assertTrue(output().contains(
                "status=completed cycles=3 new=1 duplicates=2"
        ));

        assertEquals("", errors());
    }

    @Test
    void shouldRejectInvalidArgumentsBeforeRunningLoop() {
        AtomicInteger calls = new AtomicInteger();

        String[][] invalidArguments = {
                {},
                {"--unknown"},
                {"ftp://example.invalid/feed", directory.toString(), "30", "3"},
                {"https://example.invalid/feed", "", "30", "3"},
                {"https://example.invalid/feed", directory.toString(), "0", "3"},
                {"https://example.invalid/feed", directory.toString(), "abc", "3"},
                {"https://example.invalid/feed", directory.toString(), "30", "0"},
                {"https://user:secret@example.invalid/feed",
                        directory.toString(), "30", "3"},
                {"https://example.invalid/feed#fragment",
                        directory.toString(), "30", "3"}
        };

        for (String[] args : invalidArguments) {
            int code = execute(
                    args,
                    (source, root, interval, cycles) -> {
                        calls.incrementAndGet();
                        return new GtfsRealtimeCollectionLoop.LoopSummary(
                                3, 1, 2
                        );
                    }
            );

            assertEquals(2, code);
        }

        assertEquals(0, calls.get());
        assertTrue(errors().contains("Arguments invalides"));
        assertFalse(output().contains("status=completed"));
    }

    @Test
    void shouldReturnFailureForIoError() {
        int code = execute(
                validArguments(),
                (source, root, interval, cycles) -> {
                    throw new IOException("Échec simulé");
                }
        );

        assertEquals(1, code);
        assertTrue(errors().contains("status=failed"));
        assertTrue(errors().contains("Échec simulé"));
        assertFalse(output().contains("status=completed"));
    }

    @Test
    void shouldReturnFailureForUnexpectedRuntimeError() {
        int code = execute(
                validArguments(),
                (source, root, interval, cycles) -> {
                    throw new IllegalStateException("État incohérent");
                }
        );

        assertEquals(1, code);
        assertTrue(errors().contains("IllegalStateException"));
        assertFalse(output().contains("status=completed"));
    }

    @Test
    void shouldPreserveInterruptionAndReturn130() {
        try {
            int code = execute(
                    validArguments(),
                    (source, root, interval, cycles) -> {
                        throw new InterruptedException("Arrêt simulé");
                    }
            );

            assertEquals(130, code);
            assertTrue(Thread.currentThread().isInterrupted());
            assertTrue(errors().contains("status=interrupted"));
            assertFalse(output().contains("status=completed"));
        } finally {
            Thread.interrupted();
        }
    }

    private String[] validArguments() {
        return new String[]{
                "https://example.invalid/feed",
                directory.toString(),
                "30",
                "3"
        };
    }

    private int execute(
            String[] args,
            GtfsRealtimeCollectorApplication.LoopRunner runner
    ) {
        try (
                PrintStream out = new PrintStream(
                        stdout,
                        true,
                        StandardCharsets.UTF_8
                );
                PrintStream err = new PrintStream(
                        stderr,
                        true,
                        StandardCharsets.UTF_8
                )
        ) {
            return GtfsRealtimeCollectorApplication.run(
                    args,
                    runner,
                    out,
                    err
            );
        }
    }

    private String output() {
        return stdout.toString(StandardCharsets.UTF_8);
    }

    private String errors() {
        return stderr.toString(StandardCharsets.UTF_8);
    }
}