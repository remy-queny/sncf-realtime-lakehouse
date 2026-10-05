package fr.remyqueny.sncf.producer;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SimulationReferenceCoverageTest {

    private static final Path REFERENCE_DIRECTORY =
            Path.of("..", "spark", "resources", "simulation");

    @Test
    void shouldCoverEverySimulationRoute() throws IOException {
        Set<String> routeIds = readIdentifiers(
                "routes.csv",
                "route_id,line_name"
        );

        var templates = TripUpdateEventFactory.simulationTemplates();
        assertFalse(templates.isEmpty(), "Aucun modèle de simulation");

        for (var template : templates) {
            assertTrue(
                    routeIds.contains(template.routeId()),
                    () -> "route_id absent de routes.csv : "
                            + template.routeId()
                            + " (trip_id=" + template.tripId() + ")"
            );
        }
    }

    @Test
    void shouldCoverEverySimulationStop() throws IOException {
        Set<String> stopIds = readIdentifiers(
                "stops.csv",
                "stop_id,stop_name"
        );

        var templates = TripUpdateEventFactory.simulationTemplates();
        assertFalse(templates.isEmpty(), "Aucun modèle de simulation");

        for (var template : templates) {
            assertTrue(
                    stopIds.contains(template.stopId()),
                    () -> "stop_id absent de stops.csv : "
                            + template.stopId()
                            + " (trip_id=" + template.tripId() + ")"
            );
        }
    }

    private static Set<String> readIdentifiers(
            String filename,
            String expectedHeader
    ) throws IOException {
        Path path = REFERENCE_DIRECTORY.resolve(filename)
                .toAbsolutePath()
                .normalize();

        assertTrue(
                Files.isRegularFile(path),
                () -> "Référentiel introuvable : " + path
        );

        List<String> lines = Files.readAllLines(
                path,
                StandardCharsets.UTF_8
        );

        assertFalse(lines.isEmpty(), "Référentiel vide : " + path);
        assertEquals(
                expectedHeader,
                lines.get(0).strip(),
                "En-tête inattendu : " + path
        );

        Set<String> identifiers = new HashSet<>();

        for (int index = 1; index < lines.size(); index++) {
            String line = lines.get(index).strip();

            if (line.isEmpty()) {
                continue;
            }

            String[] columns = line.split(",", -1);

            assertEquals(
                    2,
                    columns.length,
                    "Deux colonnes attendues : " + path
                            + ", ligne " + (index + 1)
            );

            String identifier = columns[0].strip();

            assertFalse(
                    identifier.isEmpty(),
                    "Identifiant vide : " + path
                            + ", ligne " + (index + 1)
            );

            identifiers.add(identifier);
        }

        return identifiers;
    }
}