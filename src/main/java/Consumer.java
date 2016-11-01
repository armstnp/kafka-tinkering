import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

public class Consumer {
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "<IPs Here>");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		int partition = Integer.parseInt(args[0], 10);

		Collection<TopicPartition> topicPartitions = new ArrayList<>();
		topicPartitions.add(new TopicPartition("my-topic", partition));

		try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
			consumer.assign(topicPartitions);
			consumer.seekToBeginning(topicPartitions);

			while (true) {
				processMessages(consumer.poll(10));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void processMessages(ConsumerRecords<String, String> messages) {
		for (ConsumerRecord record : messages) {
			System.out.println(
					String.format(
							"Topic: %s, " +
							"Partition: %d, " +
							"Offset: %d, " +
							"Key: %s, " +
							"Value: %s",
							record.topic(),
							record.partition(),
							record.offset(),
							record.key(),
							record.value()));
		}
	}
}
