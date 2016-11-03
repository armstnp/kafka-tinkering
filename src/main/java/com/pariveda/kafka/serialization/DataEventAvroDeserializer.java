package com.pariveda.kafka.serialization;

import avro.models.DataEvent;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

/**
 * Deserializes a DataEvent from bytes based on its Avro schema.
 */
public class DataEventAvroDeserializer implements Deserializer<DataEvent> {
	private static final Schema DATA_EVENT_SCHEMA = DataEvent.getClassSchema();

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) { }

	@Override
	public DataEvent deserialize(String topic, byte[] data) {
		SpecificDatumReader<DataEvent> reader = new SpecificDatumReader<>(DATA_EVENT_SCHEMA);
		Decoder decoder = DecoderFactory.get().binaryDecoder(data, null); //TODO: Examine whether reuse is possible

		try {
			return reader.read(null, decoder);
		} catch (IOException e) {
			e.printStackTrace();
			throw new IllegalArgumentException(e);
		}
	}

	@Override
	public void close() { }
}
