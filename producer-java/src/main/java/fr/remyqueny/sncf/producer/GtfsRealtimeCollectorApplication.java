package fr.remyqueny.sncf.producer;

import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.nio.file.Path;

public final class GtfsRealtimeCollectorApplication {

    private static final String USAGE =
            "Usage : GtfsRealtimeCollectorApplication "
                    + "<url-source> <dossier-collecte> "
                    + "<pause-secondes> <nombre-cycles>";

    private GtfsRealtimeCollectorApplication() {
    }

    @FunctionalInterface
    interface LoopRunner {

        GtfsRealtimeCollectionLoop.LoopSummary run(
                URI source,
                Path collectionRoot,
                long intervalSeconds,
                int maxCycles
        ) throws IOException, InterruptedException;
    }

    private record Options(
            URI source,
            Path collectionRoot,
            long intervalSeconds,
            int maxCycles
    ) {
    }

    public static void main(String[] args) {
        int code = run(
                args,
                new GtfsRealtimeCollectionLoop()::run,
                System.out,
                System.err
        );

        if (code != 0) {
            System.exit(code);
        }
    }

    static int run(
            String[] args,
            LoopRunner runner,
            PrintStream out,
            PrintStream err
    ) {
        if (args.length == 1 && "--help".equals(args[0])) {
            out.println(USAGE);
            out.println(
                    "La pause s'applique après chaque cycle, "
                            + "sauf le dernier."
            );
            out.println(
                    "Utiliser un dossier dédié à une seule source."
            );
            out.println(
                    "Collecte brute uniquement : aucun export JSONL, "
                            + "envoi Kafka ou traitement Spark."
            );
            return 0;
        }

        Options options;

        try {
            options = parseOptions(args);
        } catch (IllegalArgumentException exception) {
            err.println("Arguments invalides : " + exception.getMessage());
            err.println(USAGE);
            return 2;
        }

        try {
            var summary = runner.run(
                    options.source(),
                    options.collectionRoot(),
                    options.intervalSeconds(),
                    options.maxCycles()
            );

            out.println(
                    "status=completed"
                            + " cycles=" + summary.completedCycles()
                            + " new=" + summary.newSnapshots()
                            + " duplicates=" + summary.duplicateSnapshots()
            );

            return 0;
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            err.println("status=interrupted");
            return 130;
        } catch (IOException | RuntimeException exception) {
            err.println(
                    "status=failed"
                            + " exception=" + exception.getClass().getSimpleName()
                            + " message=" + exception.getMessage()
            );

            return 1;
        }
    }

    private static Options parseOptions(String[] args) {
        if (args.length != 4) {
            throw new IllegalArgumentException(
                    "Quatre arguments sont attendus."
            );
        }

        URI source = URI.create(args[0]);

        if (source.getHost() == null
                || (!"http".equalsIgnoreCase(source.getScheme())
                && !"https".equalsIgnoreCase(source.getScheme()))) {
            throw new IllegalArgumentException(
                    "Une URL HTTP ou HTTPS avec un hôte est requise."
            );
        }

        if (source.getUserInfo() != null || source.getFragment() != null) {
            throw new IllegalArgumentException(
                    "L'URL ne doit contenir ni identifiants intégrés "
                            + "ni fragment."
            );
        }

        if (args[1].isBlank()) {
            throw new IllegalArgumentException(
                    "Le dossier de collecte ne doit pas être vide."
            );
        }

        Path collectionRoot = Path.of(args[1])
                .toAbsolutePath()
                .normalize();

        long intervalSeconds = Long.parseLong(args[2]);
        int maxCycles = Integer.parseInt(args[3]);

        if (intervalSeconds <= 0
                || intervalSeconds > Long.MAX_VALUE / 1000) {
            throw new IllegalArgumentException(
                    "Pause invalide."
            );
        }

        if (maxCycles <= 0) {
            throw new IllegalArgumentException(
                    "Le nombre de cycles doit être strictement positif."
            );
        }

        return new Options(
                source,
                collectionRoot,
                intervalSeconds,
                maxCycles
        );
    }
}