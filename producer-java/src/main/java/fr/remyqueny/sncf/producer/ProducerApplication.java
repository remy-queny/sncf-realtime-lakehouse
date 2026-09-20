package fr.remyqueny.sncf.producer;

public final class ProducerApplication {

    private ProducerApplication() {
    }

    public static void main(String[] args) {
        AppConfig config = AppConfig.fromEnvironment();

        System.out.println("SNCF Kafka producer configuration:");
        System.out.println("Kafka bootstrap servers: " + config.bootstrapServers());
        System.out.println("Kafka trip updates topic: " + config.tripUpdatesTopic());
        System.out.println("Simulation mode: " + config.simulationMode());
    }
}