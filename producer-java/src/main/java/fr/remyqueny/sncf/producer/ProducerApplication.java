package fr.remyqueny.sncf.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ProducerApplication {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerApplication.class);

    private ProducerApplication() {
    }

    public static void main(String[] args) {
        try {
            run();
        } catch (Exception exception) {
            LOGGER.error(
                "status=failed exception_type={} message={}",
                exception.getClass().getSimpleName(),
                exception.getMessage(),
                exception
                );
            System.exit(1);
        }
    }

    private static void run() throws Exception {
        AppConfig config = AppConfig.fromEnvironment();

        if (!config.simulationMode()) {
            throw new IllegalStateException(
                    "SIMULATION_MODE=false is not implemented yet."
            );
        }

        TripUpdateEvent event = TripUpdateEventFactory.createSimulatedEvent();
        String eventJson = new ObjectMapper().writeValueAsString(event);

        Properties properties = new Properties();

        properties.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                config.bootstrapServers()
        );
        properties.put(
                ProducerConfig.CLIENT_ID_CONFIG,
                "sncf-java-producer"
        );
        properties.put(
                ProducerConfig.ACKS_CONFIG,
                "all"
        );
        properties.put(
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
                true
        );
        properties.put(
                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,
                10_000
        );
        properties.put(
                ProducerConfig.MAX_BLOCK_MS_CONFIG,
                30_000
        );
        properties.put(
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,
                30_000
        );
        properties.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName()
        );
        properties.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName()
        );

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    config.tripUpdatesTopic(),
                    event.tripId(),
                    eventJson
            );

            RecordMetadata metadata = producer.send(record).get();

            LOGGER.info(
                "status=delivered event_id={} topic={} partition={} offset={}",
                event.eventId(),
                metadata.topic(),
                metadata.partition(),
                metadata.offset()
                );
        }
    }
}