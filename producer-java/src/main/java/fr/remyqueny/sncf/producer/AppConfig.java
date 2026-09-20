package fr.remyqueny.sncf.producer;

public record AppConfig(
        String bootstrapServers,
        String tripUpdatesTopic,
        boolean simulationMode
) {

    public static AppConfig fromEnvironment() {
        return new AppConfig(
                getEnvOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                getEnvOrDefault("KAFKA_TRIP_UPDATES_TOPIC", "sncf.trip_updates.raw"),
                Boolean.parseBoolean(
                        getEnvOrDefault("SIMULATION_MODE", "true")
                )
        );
    }

    private static String getEnvOrDefault(String variableName, String defaultValue) {
        String value = System.getenv(variableName);

        if (value == null || value.isBlank()) {
            return defaultValue;
        }

        return value;
    }
}