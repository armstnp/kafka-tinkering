package com.pariveda.kafka.serialization;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Serializes an Avro generic data record into bytes.
 *
 * Expected configurations:
 *   - If serializing a key:
 *     * 'key.serializer.avro.schema': The writer schema for the key
 *   - If serializing a value:
 *     * 'value.serializer.avro.schema': The writer schema for the value
 */
public class AvroSerializer implements Serializer<GenericData.Record> {
	private Injection<GenericData.Record, byte[]> avroBinaryInjection;

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		String schemaConfigType = isKey ? "key" : "value";
		String schemaConfigKey = schemaConfigType + ".serializer.avro.schema";
		String schemaString = (String) configs.get(schemaConfigKey);

		if(schemaString == null) throw new IllegalArgumentException("No Avro schema provided for the " + schemaConfigType);

		Schema schema = new Schema.Parser().parse(schemaString);
		avroBinaryInjection = GenericAvroCodecs.toBinary(schema);
	}

	@Override
	public byte[] serialize(String topic, GenericData.Record data) {
		return avroBinaryInjection.apply(data);
	}

	@Override
	public void close() { }
}
