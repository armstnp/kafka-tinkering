package com.pariveda.kafka.serialization

import com.twitter.bijection.Injection
import com.twitter.bijection.avro.GenericAvroCodecs
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.kafka.common.serialization.Serializer

/**
 * Serializes an Avro generic data record into bytes.

 * Expected configurations:
 * - If serializing a key:
 * * 'key.serializer.avro.schema': The writer schema for the key
 * - If serializing a value:
 * * 'value.serializer.avro.schema': The writer schema for the value
 */
class KAvroSerializer : Serializer<GenericData.Record> {
    private var avroBinaryInjection: Injection<GenericData.Record, ByteArray>? = null

    override fun configure(configs: Map<String, *>, isKey: Boolean) {
        val schemaConfigType = if (isKey) "key" else "value"
        val schemaConfigKey = "$schemaConfigType.serializer.avro.schema"

        val schemaString = configs.getOrElse(schemaConfigKey) {
            throw IllegalArgumentException("No Avro schema provided for the " + schemaConfigType)
        }

        val schema = Schema.Parser().parse(schemaString as String)
        avroBinaryInjection = GenericAvroCodecs.toBinary<GenericData.Record>(schema)
    }

    override fun serialize(topic: String, data: GenericData.Record) = avroBinaryInjection!!.apply(data)

    override fun close() { }
}
