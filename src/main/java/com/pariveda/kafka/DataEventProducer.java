package com.pariveda.kafka;

import avro.models.DataEvent;
import com.google.common.util.concurrent.RateLimiter;
import com.pariveda.kafka.common.ConfigurationWrapper;
import com.pariveda.kafka.common.DataEventFactory;
import com.pariveda.kafka.common.StatsDClientFactory;
import com.timgroup.statsd.StatsDClient;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class DataEventProducer {
	private static final String PRODUCER_PROPERTIES = "producer.properties";
	private static final Logger log = LoggerFactory.getLogger(DataEventProducer.class);

	private static ConfigurationWrapper config;
	private static StatsDClient statsd;

	public static void main(String[] args) {
		try {
			config = new ConfigurationWrapper(PRODUCER_PROPERTIES);
			statsd = (new StatsDClientFactory(config)).getStatsDClient();
		} catch (ConfigurationException ex) {
			log.error("Configuration exception occurred: {}", ex.getMessage());
			System.exit(1);
		}

		Properties props = config.getPropertiesFromNamespace("kafka");

		int eventsPerSecond = Integer.parseInt(args[0], 10);
		RateLimiter limiter = RateLimiter.create(eventsPerSecond);

		try (KafkaProducer<String, DataEvent> producer = new KafkaProducer<>(props)) {
			while (true) {
				limiter.acquire();

				DataEvent dataEvent = DataEventFactory.generateDataEvent();
				ProducerRecord<String, DataEvent> record = new ProducerRecord<>("data-event-source", buildKeyFromDataEvent(dataEvent), dataEvent);
				producer.send(record);

				statsd.incrementCounter("produced-data-event");
				log.info("Sent data event: {}", dataEvent);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static String buildKeyFromDataEvent(DataEvent dataEvent) {
		return String.format("%s:%s", dataEvent.getDatabase(), dataEvent.getTable());
	}
}
