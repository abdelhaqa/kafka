package raiskumar.com.github.producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class EventPublisher {
    public void publish(String topic, String message){
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);
        KafkaSimpleProducer.getInstance().send(record, new ProducerCallBack());
    }

    private static class ProducerCallBack implements org.apache.kafka.clients.producer.Callback {
        public ProducerCallBack() { }
        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if(exception !=null){
                System.out.println(metadata.topic()+metadata.offset()+metadata.partition());
            }
        }
    }
}
