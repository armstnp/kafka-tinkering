package com.pariveda.kafka

import avro.models.DataEvent
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import java.util.*

fun main(args: Array<String>) {
    val props = Properties().apply {
        put("bootstrap.servers", "35.162.160.212:9092,35.162.87.170:9092")
        put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        put("value.deserializer", "com.pariveda.kafka.serialization.DataEventAvroDeserializer")
    }

    if(args.size < 2) {
        System.out.println("Usage: java AvroConsumer <topic> <partition #>")
        System.exit(1)
    }

    val topic = args[0]
    val partition = args[1].toInt()

    val topicPartitions = listOf(TopicPartition(topic, partition))

    try {
        KafkaConsumer<String, DataEvent>(props).use { consumer ->
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

private fun processMessages(messages: ConsumerRecords<String, DataEvent>) =
        messages.forEach {
            System.out.println(
                    "Topic: ${it.topic()}, " +
                    "Partition: ${it.partition()}, " +
                    "Offset: ${it.offset()}, " +
                    "Key: ${it.key()}, " +
                    "Value: ${it.value()}")
        }