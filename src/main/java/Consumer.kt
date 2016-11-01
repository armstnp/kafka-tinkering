import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import java.util.*

fun main(args: Array<String>) {
    val props = Properties().apply {
        put("bootstrap.servers", "<IPs Here>")
        put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    }

    val partition = args[0].toInt()
    val topicPartitions = listOf(TopicPartition("my-topic", partition))

    try {
        KafkaConsumer<String, String>(props).use { consumer ->
            consumer.assign(topicPartitions)
            consumer.seekToBeginning(topicPartitions)

            while (true) {
                val messages = consumer.poll(10)
                processMessages(messages)
            }
        }
    } catch(e: Exception) {
        e.printStackTrace()
    }
}

private fun processMessages(messages: ConsumerRecords<String, String>) =
        messages.forEach {
            System.out.println(
                    "Topic: ${it.topic()}, " +
                    "Partition: ${it.partition()}, " +
                    "Offset: ${it.offset()}, " +
                    "Key: ${it.key()}, " +
                    "Value: ${it.value()}")
        }