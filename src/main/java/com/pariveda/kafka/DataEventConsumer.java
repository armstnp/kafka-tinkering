package com.pariveda.kafka;

import avro.models.DataEvent;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Properties;

public class DataEventConsumer {
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "35.162.160.212:9092,35.162.87.170:9092");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "com.pariveda.kafka.serialization.DataEventAvroDeserializer");

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