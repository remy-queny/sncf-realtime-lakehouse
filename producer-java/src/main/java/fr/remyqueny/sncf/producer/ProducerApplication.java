package fr.remyqueny.sncf.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public final class ProducerApplication {

    private ProducerApplication() {
    }

    public static void main(String[] args) throws Exception {
        AppConfig config = AppConfig.fromEnvironment();

        if (!config.simulationMode()) {
            throw new IllegalStateException(
                    "SIMULATION_MODE=false is not implemented yet."
            );
        }

        TripUpdateEvent event = TripUpdateEventFactory.createSimulatedEvent();
        String eventJson = new ObjectMapper().writeValueAsString(event);

        Properties properties = new Properties();
        properties.put("bootstrap.servers", config.bootstrapServers());
        properties.put("client.id", "sncf-java-producer");
        properties.put("acks", "all");
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    config.tripUpdatesTopic(),
                    event.tripId(),
                    eventJson
            );

            RecordMetadata metadata = producer.send(record).get();

            System.out.printf(
                    "Delivered event_id=%s topic=%s partition=%d offset=%d%n",
                    event.eventId(),
                    metadata.topic(),
                    metadata.partition(),
                    metadata.offset()
            );
        }
    }
}