package com.pariveda.kafka.serialization

import com.twitter.bijection.Injection
import com.twitter.bijection.avro.GenericAvroCodecs
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.kafka.common.serialization.Deserializer

/**
 * Deerializes an Avro generic data record from bytes.

 * Expected configurations:
 * - If deserializing a key:
 * * 'key.deserializer.avro.schema': The reader schema for the key
 * - If deserializing a value:
 * * 'value.serializer.avro.schema': The reader schema for the value
 */
class KAvroDeserializer : Deserializer<GenericData.Record> {
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

    override fun deserialize(topic: String, data: ByteArray) = avroBinaryInjection!!.invert(data).get()

    override fun close() {
    }
}
