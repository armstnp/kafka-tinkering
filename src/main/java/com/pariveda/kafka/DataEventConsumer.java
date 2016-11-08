package com.pariveda.kafka;

import avro.models.DataEvent;
import com.pariveda.kafka.helpers.ConfigurationHelper;
import com.pariveda.kafka.helpers.StatsDHelper;
import com.timgroup.statsd.StatsDClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Properties;

public class DataEventConsumer {
	private static final StatsDClient statsd = StatsDHelper.getConsumerStatsDClient();

	public static void main(String[] args) {
		statsd.incrementCounter("ran-data-event-consumer");
		Properties props = ConfigurationHelper.getConsumerProperties("kafka");

		if (args.length < 2) {
			System.out.println("Usage: java DataEventConsumer <topic> <partition #>");
			System.exit(1);
		}

		String topic = args[0];
		int partition = Integer.parseInt(args[1], 10);

		ArrayList<TopicPartition> topicPartitions = new ArrayList<>();
		topicPartitions.add(new TopicPartition(topic, partition));

		try (KafkaConsumer<String, DataEvent> consumer = new KafkaConsumer<>(props)) {
			consumer.assign(topicPartitions);
			consumer.seekToBeginning(topicPartitions);

			while (true) {
				ConsumerRecords<String, DataEvent> messages = consumer.poll(10);
				processMessages(messages);
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
	}

	private static void processMessages(ConsumerRecords<String, DataEvent> messages) {
		for (ConsumerRecord<String, DataEvent> message : messages) {
			System.out.println(
					String.format(
							"Topic: %s, Partition: %s, Offset: %s, Key: %s, Value: %s",
							message.topic(),
							message.partition(),
							message.offset(),
							message.key(),
							message.value()));
		}
	}
}