package fr.remyqueny.sncf.producer;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public final class TripUpdateEventFactory {

    private static final List<SimulationTemplate> SIMULATION_TEMPLATES = List.of(
            new SimulationTemplate("TRAIN-H-001", "H", "stop_87271007", 480),
            new SimulationTemplate("TRAIN-J-002", "J", "stop_87271003", 720),
            new SimulationTemplate("TRAIN-R-003", "R", "stop_87271005", 120),
            new SimulationTemplate("TRAIN-L-004", "L", "stop_87113001", 600),
            new SimulationTemplate("TRAIN-H-005", "H", "stop_87271010", 0)
    );

    private TripUpdateEventFactory() {
    }

    public static TripUpdateEvent createSimulatedEvent() {
        SimulationTemplate template = SIMULATION_TEMPLATES.get(
                ThreadLocalRandom.current().nextInt(SIMULATION_TEMPLATES.size())
        );

        Instant eventTime = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        Instant scheduledTime = eventTime.plus(5, ChronoUnit.MINUTES);
        Instant estimatedTime = scheduledTime.plusSeconds(template.delaySeconds());

        return new TripUpdateEvent(
                UUID.randomUUID().toString(),
                "trip_update",
                "simulation_java",
                1,
                template.tripId(),
                template.routeId(),
                template.stopId(),
                scheduledTime.toString(),
                estimatedTime.toString(),
                template.delaySeconds(),
                eventTime.toString(),
                Instant.now().truncatedTo(ChronoUnit.SECONDS).toString()
        );
    }

    private record SimulationTemplate(
            String tripId,
            String routeId,
            String stopId,
            int delaySeconds
    ) {
    }
}