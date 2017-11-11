package raiskumar.com.github.producer;

import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

public class KafkaSimpleProducer {
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";
    private static KafkaProducer<String, String> producer;

    private KafkaSimpleProducer(){
    }

    static {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        kafkaProps.put("acks", "all");
        kafkaProps.put("compression.type", "snappy");
        kafkaProps.put("retries", 1);
        kafkaProps.put("batch.size", 16384);
        kafkaProps.put("linger.ms", 5);
        kafkaProps.put("buffer.memory", 33554432);
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(kafkaProps);
    }

    public static KafkaProducer<String, String> getInstance(){
       return producer;
    }
}
