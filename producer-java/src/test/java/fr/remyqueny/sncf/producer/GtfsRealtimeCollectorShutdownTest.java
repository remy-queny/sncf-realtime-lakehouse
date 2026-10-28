package fr.remyqueny.sncf.producer;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GtfsRealtimeCollectorShutdownTest {

    @Test
    void shouldInterruptWaitingCollectorAndConfirmTermination()
            throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        AtomicBoolean interrupted = new AtomicBoolean();
        AtomicBoolean shutdownRequested = new AtomicBoolean();
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        Thread collector = new Thread(() -> {
            started.countDown();
            try {
                Thread.sleep(60000);
            } catch (InterruptedException exception) {
                interrupted.set(true);
                Thread.currentThread().interrupt();
            }
        });

        collector.setDaemon(true);
        collector.start();

        try (PrintStream err = new PrintStream(
                output, true, StandardCharsets.UTF_8
        )) {
            assertTrue(started.await(5, TimeUnit.SECONDS));

            GtfsRealtimeCollectorApplication.stopCollector(
                    collector, shutdownRequested, err, 5000
            );

            assertTrue(shutdownRequested.get());
            assertTrue(interrupted.get());
            assertFalse(collector.isAlive());
            assertTrue(text(output).contains("status=shutdown_requested"));
            assertTrue(text(output).contains("status=shutdown_finished"));
            assertFalse(text(output).contains("status=shutdown_timeout"));
        } finally {
            collector.interrupt();
            collector.join(5000);
        }
    }

    @Test
    void shouldReportTimeoutWhenCollectorDoesNotStop()
            throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean shutdownRequested = new AtomicBoolean();
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        Thread collector = new Thread(() -> {
            started.countDown();
            boolean released = false;

            while (!released) {
                try {
                    release.await();
                    released = true;
                } catch (InterruptedException exception) {
                    // Deliberately ignore interruption to exercise timeout.
                }
            }
        });

        collector.setDaemon(true);
        collector.start();

        try (PrintStream err = new PrintStream(
                output, true, StandardCharsets.UTF_8
        )) {
            assertTrue(started.await(5, TimeUnit.SECONDS));

            GtfsRealtimeCollectorApplication.stopCollector(
                    collector, shutdownRequested, err, 25
            );

            assertTrue(shutdownRequested.get());
            assertTrue(collector.isAlive());
            assertTrue(text(output).contains("status=shutdown_timeout"));
            assertFalse(text(output).contains("status=shutdown_finished"));
        } finally {
            release.countDown();
            collector.join(5000);
        }
    }

    @Test
    void shouldHandleAlreadyTerminatedCollector() throws Exception {
        Thread collector = new Thread(() -> {
        });
        collector.setDaemon(true);
        collector.start();
        collector.join(5000);
        assertFalse(collector.isAlive());

        AtomicBoolean shutdownRequested = new AtomicBoolean();
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        try (PrintStream err = new PrintStream(
                output, true, StandardCharsets.UTF_8
        )) {
            GtfsRealtimeCollectorApplication.stopCollector(
                    collector, shutdownRequested, err, 5000
            );
        }

        assertTrue(shutdownRequested.get());
        assertTrue(text(output).contains("status=shutdown_finished"));
        assertFalse(text(output).contains("status=shutdown_timeout"));
    }

    @Test
    void shouldRejectNonPositiveShutdownWait() {
        AtomicBoolean shutdownRequested = new AtomicBoolean();
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        Thread collector = new Thread(() -> {
        });

        try (PrintStream err = new PrintStream(
                output, true, StandardCharsets.UTF_8
        )) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> GtfsRealtimeCollectorApplication.stopCollector(
                            collector, shutdownRequested, err, 0
                    )
            );
        }

        assertFalse(shutdownRequested.get());
    }

    private static String text(ByteArrayOutputStream output) {
        return output.toString(StandardCharsets.UTF_8);
    }
}