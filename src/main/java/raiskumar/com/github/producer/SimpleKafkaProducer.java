package raiskumar.com.github.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class SimpleKafkaProducer {
    private final static String TOPIC = "test";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";
    private String fileName = "/Applications/kafka_2.11-0.11.0.1/sampleFile.txt";
    private static KafkaProducer<String, String> producer;


    private static KafkaProducer<String, String> getProducer(){
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
        return producer;
    }

    public void publish(){
        KafkaProducer<String, String> producer = getProducer();
        long t1 = System.currentTimeMillis();
        long index = 0;
        try(BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            for(String line; (line = br.readLine()) != null; ) {
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, line);
                producer.send(record, new ProducerCallBack(index, line));
                index++;
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Throwable e){
            e.printStackTrace();
        }finally{
            producer.close();
            long t2 = System.currentTimeMillis();
            System.out.println(" time taken ="+ (t2-t1));
            System.out.println(" Number or records processed ="+ index);
        }
    }

    private static class ProducerCallBack implements org.apache.kafka.clients.producer.Callback {

        public ProducerCallBack(long index, String line) {
            System.out.println(" index="+ index + " & line ="+ line);
        }

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if(exception !=null){
                System.out.println(metadata.topic()+metadata.offset()+metadata.partition());
            }
        }
    }

    public static void main(String[] args){
        SimpleKafkaProducer obj = new SimpleKafkaProducer();
        obj.publish();
    }

}
