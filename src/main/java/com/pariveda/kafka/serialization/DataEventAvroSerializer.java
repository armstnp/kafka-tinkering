package com.pariveda.kafka.serialization;

import avro.models.DataEvent;
import org.apache.avro.Schema;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

/**
 * Serializes a DataEvent into bytes based on its Avro schema.
 */
public class DataEventAvroSerializer implements Serializer<DataEvent> {
	private static final Schema DATA_EVENT_SCHEMA = DataEvent.getClassSchema();

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) { }

	@Override
	public byte[] serialize(String topic, DataEvent event) {
		try(ByteArrayOutputStream out = new ByteArrayOutputStream()) {
			BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null); //TODO: Determine whether reuse is viable
			DatumWriter<DataEvent> writer = new SpecificDatumWriter<>(DATA_EVENT_SCHEMA);

			writer.write(event, encoder);
			encoder.flush();
			out.close();
			return out.toByteArray();
		} catch (IOException e) {
			e.printStackTrace();
			throw new IllegalArgumentException(e);
		}
	}

	@Override
	public void close() { }
}
