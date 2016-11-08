package com.pariveda.kafka;

import avro.models.DataEvent;
import com.google.common.util.concurrent.RateLimiter;
import com.pariveda.kafka.helpers.ConfigurationHelper;
import com.pariveda.kafka.helpers.DataEventHelper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class Producer {
	public static void main(String[] args) {
		Properties props = ConfigurationHelper.getProducerProperties("kafka");

		int eventsPerSecond = Integer.parseInt(args[0], 10);
		RateLimiter limiter = RateLimiter.create(eventsPerSecond);

		try (KafkaProducer<String, DataEvent> producer = new KafkaProducer<>(props)) {
			while (true) {
				limiter.acquire();

				DataEvent dataEvent = DataEventHelper.generateDataEvent();
				ProducerRecord<String, DataEvent> record = new ProducerRecord<>("data-event-source", buildKeyFromDataEvent(dataEvent), dataEvent);
				producer.send(record);

				System.out.println("Sent data event: " + dataEvent);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static String buildKeyFromDataEvent(DataEvent dataEvent) {
		return String.format("%s:%s", dataEvent.getDatabase(), dataEvent.getTable());
	}
}
