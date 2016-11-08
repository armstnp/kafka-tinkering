package com.pariveda.kafka;

import avro.models.DataEvent;
import com.google.common.util.concurrent.RateLimiter;
import com.pariveda.kafka.helpers.ConfigurationHelper;
import com.pariveda.kafka.helpers.DataEventHelper;
import com.pariveda.kafka.helpers.StatsDHelper;
import com.timgroup.statsd.StatsDClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer {
	private static final StatsDClient statsd = StatsDHelper.getProducerStatsDClient();
	private static final Logger log = LoggerFactory.getLogger(Producer.class);

	public static void main(String[] args) {
		statsd.incrementCounter("ran-data-event-producer");
		Properties props = ConfigurationHelper.getProducerProperties("kafka");

		int eventsPerSecond = Integer.parseInt(args[0], 10);
		RateLimiter limiter = RateLimiter.create(eventsPerSecond);

		try (KafkaProducer<String, DataEvent> producer = new KafkaProducer<>(props)) {
			while (true) {
				limiter.acquire();

				DataEvent dataEvent = DataEventHelper.generateDataEvent();
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
