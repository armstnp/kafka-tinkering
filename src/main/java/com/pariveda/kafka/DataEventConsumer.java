package com.pariveda.kafka;

import avro.models.DataEvent;
import com.pariveda.kafka.common.ConfigurationWrapper;
import com.pariveda.kafka.common.StatsDClientFactory;
import com.timgroup.statsd.StatsDClient;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Properties;

public class DataEventConsumer {
	private static final String CONSUMER_PROPERTIES = "consumer.properties";
	private static final Logger log = LoggerFactory.getLogger(DataEventProducer.class);

	private static ConfigurationWrapper config;
	private static StatsDClient statsd;

	public static void main(String[] args) {
		if (args.length < 2) {
			System.out.println("Usage: java DataEventConsumer <topic> <partition #>");
			System.exit(1);
		}

		try {
			config = new ConfigurationWrapper(CONSUMER_PROPERTIES);
			statsd = (new StatsDClientFactory(config)).getStatsDClient();
		} catch (ConfigurationException ex) {
			log.error("Configuration exception occurred: {}", ex.getMessage());
			System.exit(1);
		}

		Properties props = config.getPropertiesFromNamespace("kafka");

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