import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.*

fun main(args: Array<String>) {
    val props = Properties().apply {
        put("bootstrap.servers", "35.162.160.212:9092,35.162.87.170:9092")
        put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    }

    try {
        KafkaProducer<String, String>(props).use {
            (1..150).forEach { i ->
                it.send(ProducerRecord("my-topic", "$i", "My Message: $i"))
            }
        }
    } catch(e: Exception) {
        e.printStackTrace()
    }
}