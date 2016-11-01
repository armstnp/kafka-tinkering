import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class Producer {
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "<IPs Here>");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
			for(int i = 1; i < 150; i++) {
				producer.send(new ProducerRecord<>("my-topic", Integer.toString(i), "My Message: " + i));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
