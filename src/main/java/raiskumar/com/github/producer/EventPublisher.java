package raiskumar.com.github.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class EventPublisher {
    private KafkaProducer<String, String> producer = KafkaSimpleProducer.getInstance();

    public void publish(String topic, String message){
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);
        producer.send(record, new ProducerCallBack(message));
    }

    private static class ProducerCallBack implements org.apache.kafka.clients.producer.Callback {
        public ProducerCallBack(String message) {
            System.out.println("message ="+ message);
        }
        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if(exception !=null){
                System.out.println(metadata.topic()+metadata.offset()+metadata.partition());
            }
        }
    }
}
