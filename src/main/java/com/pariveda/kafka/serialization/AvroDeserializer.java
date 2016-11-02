package com.pariveda.kafka.serialization;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * Deerializes an Avro generic data record from bytes.
 *
 * Expected configurations:
 * - If deserializing a key:
 * * 'key.deserializer.avro.schema': The reader schema for the key
 * - If deserializing a value:
 * * 'value.serializer.avro.schema': The reader schema for the value
 */
public class AvroDeserializer implements Deserializer<GenericData.Record> {
	private Injection<GenericData.Record, byte[]> avroBinaryInjection;

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		String schemaConfigType = isKey ? "key" : "value";
		String schemaConfigKey = schemaConfigType + ".deserializer.avro.schema";
		String schemaString = (String) configs.get(schemaConfigKey);

		if(schemaString == null) throw new IllegalArgumentException("No Avro schema provided for the " + schemaConfigType);

		Schema schema = new Schema.Parser().parse(schemaString);
		avroBinaryInjection = GenericAvroCodecs.toBinary(schema);
	}

	@Override
	public GenericData.Record deserialize(String topic, byte[] data) {
		return avroBinaryInjection.invert(data).get();
	}

	@Override
	public void close() { }
}
