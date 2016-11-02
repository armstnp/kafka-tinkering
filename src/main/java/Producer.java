import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.ByteArrayOutputStream;
import java.util.Properties;

import avro.models.DataEvent;

public class Producer {
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "35.162.160.212:9092,35.162.87.170:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

		try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props)) {
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

			ByteArrayOutputStream out = new ByteArrayOutputStream();
			BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
			DatumWriter<DataEvent> writer = new SpecificDatumWriter<DataEvent>(DataEvent.getClassSchema());

			writer.write(dataEvent, encoder);
			encoder.flush();
			out.close();

			byte[] serializedBytes = out.toByteArray();

			//String eventString = dataEvent.toString();
			//System.out.println(eventString);

			producer.send(new ProducerRecord<>("my-topic", Integer.toString(1), serializedBytes));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
