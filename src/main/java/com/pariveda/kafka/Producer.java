package com.pariveda.kafka;

import avro.models.DataEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class Producer {
	public static void main(String[] args) {
		Properties props = new Properties();

		props.put("bootstrap.servers", "35.162.160.212:9092,35.162.87.170:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "com.pariveda.kafka.serialization.DataEventAvroSerializer");

		try (KafkaProducer<String, DataEvent> producer = new KafkaProducer<>(props)) {
			DataEvent dataEvent = DataEvent.newBuilder()
									.setEventId(1)
									.setTimestamp("timestamp")
									.setDatabase("database")
									.setTable("table")
									.setColumn("column")
									.setOldValue("oldValue")
									.setNewValue("newValue")
									.build();

			ProducerRecord<String, DataEvent> record = new ProducerRecord<>("data-event-source", "database:table", dataEvent);
			producer.send(record);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
