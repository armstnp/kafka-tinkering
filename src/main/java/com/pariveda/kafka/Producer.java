package com.pariveda.kafka;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;
import avro.models.DataEvent;

public class Producer {
	public static void main(String[] args) {

		String DATA_SCHEMA = "{" +
				"\"namespace\": \"avro.models\"," +
				"\"type\": \"record\"," +
				"\"name\": \"DataEvent\"," +
				"\"fields\": [" +
					"{\"name\": \"event_id\", \"type\": \"int\"}," +
					"{\"name\": \"database\", \"type\": \"string\"}," +
					"{\"name\": \"table\", \"type\": \"string\"}," +
					"{\"name\": \"column\", \"type\": \"string\"}," +
					"{\"name\": \"old_value\", \"type\": \"string\"}," +
					"{\"name\": \"new_value\", \"type\": \"string\"}" +
				"]" +
			"}";

		Properties props = new Properties();

		props.put("bootstrap.servers", "35.162.160.212:9092,35.162.87.170:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "com.pariveda.kafka.serialization.AvroSerializer");
		props.put("value.serializer.avro.schema", DATA_SCHEMA);

		Schema.Parser parser = new Schema.Parser();
		Schema schema = parser.parse(DATA_SCHEMA);
		Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);

		try (KafkaProducer<String, DataEvent> producer = new KafkaProducer<>(props)) {
			//for(int i = 1; i < 150; i++) {
			//	producer.send(new ProducerRecord<>("my-topic", Integer.toString(i), "My Message: " + i));
			//}

			DataEvent dataEvent = DataEvent.newBuilder()
									.setEventId(1)
									.setTimestamp("timestamp")
									.setDatabase("database")
									.setTable("table")
									.setColumn("column")
									.setOldValue("oldValue")
									.setNewValue("newValue")
									.build();

			GenericData.Record avroRecord = new GenericData.Record(schema);
			avroRecord.

			byte[] bytes = recordInjection.apply(avroRecord);

			ProducerRecord record = new ProducerRecord<>("my-topic", bytes);
			producer.send(record);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
