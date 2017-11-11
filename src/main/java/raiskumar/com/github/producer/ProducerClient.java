package raiskumar.com.github.producer;

public class ProducerClient {
    public static void main(String[] args){
        EventPublisher ep = new EventPublisher();
        ep.publish("test", "test message !");
    }
}
