package com.pariveda.kafka.serialization;

import avro.models.DataEvent;
import org.apache.avro.Schema;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

/**
 * Deserializes a DataEvent from bytes based on its Avro schema.
 */
public class DataEventAvroDeserializer implements Deserializer<DataEvent> {
	private static final Schema DATA_EVENT_SCHEMA = DataEvent.getClassSchema();
	private static final SpecificDatumReader<DataEvent> DATA_EVENT_DATUM_READER =
			new SpecificDatumReader<>(DATA_EVENT_SCHEMA);

	private BinaryDecoder mostRecentDecoder = null;

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) { }

	@Override
	public DataEvent deserialize(String topic, byte[] data) {
		Decoder decoder = createDecoder(data);

		try {
			return DATA_EVENT_DATUM_READER.read(null, decoder); //TODO: Determine whether to reuse model
		} catch (IOException e) {
			e.printStackTrace();
			throw new IllegalArgumentException(e);
		}
	}

	private Decoder createDecoder(byte[] data) {
		mostRecentDecoder = DecoderFactory.get().binaryDecoder(data, mostRecentDecoder);
		return mostRecentDecoder;
	}

	@Override
	public void close() { }
}
