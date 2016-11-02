package com.pariveda.kafka

import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import java.util.*

val schema = """{"namespace": "avro.models",
  "type": "record",
  "name": "DataEvent",
  "fields": [
    {"name": "event_id", "type": "int"},
    {"name": "database", "type": "string"},
    {"name": "table", "type": "string"},
    {"name": "column", "type": "string"},
    {"name": "old_value", "type": "string"},
    {"name": "new_value", "type": "string"}
  ]
}"""

data class DataEvent(
        val eventId: Int,
        val database: String,
        val table: String,
        val column: String,
        val oldValue: String,
        val newValue: String)

fun main(args: Array<String>) {
    val props = Properties().apply {
        put("bootstrap.servers", "<IPs Here>")
        put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        put("value.deserializer", "com.pariveda.kafka.serialization.KAvroDeserializer")
        put("value.deserializer.avro.schema", schema)
    }

    if(args.size < 2) {
        System.out.println("Usage: java AvroConsumer <topic> <partition #>")
        System.exit(1)
    }

    val topic = args[0]
    val partition = args[1].toInt()

    val topicPartitions = listOf(TopicPartition(topic, partition))

    try {
        KafkaConsumer<String, GenericData.Record>(props).use { consumer ->
            consumer.assign(topicPartitions)
            consumer.seekToBeginning(topicPartitions)

            while (true) {
                val messages = consumer.poll(10)
                processMessages(messages)
            }
        }
    } catch(e: Exception) {
        e.printStackTrace()
        throw e
    }
}

private fun processMessages(messages: ConsumerRecords<String, GenericData.Record>) =
        messages.forEach {
            val dataEvent = parseDataEventFromRecord(it.value())

            System.out.println(
                    "Topic: ${it.topic()}, " +
                    "Partition: ${it.partition()}, " +
                    "Offset: ${it.offset()}, " +
                    "Key: ${it.key()}, " +
                    "Value: $dataEvent")
        }

private fun parseDataEventFromRecord(record: GenericData.Record): DataEvent {
    val eventId = record["event_id"] as Int
    val database = record["database"] as String
    val table = record["table"] as String
    val column = record["column"] as String
    val oldValue = record["old_value"] as String
    val newValue = record["new_value"] as String

    return DataEvent(eventId, database, table, column, oldValue, newValue)
}