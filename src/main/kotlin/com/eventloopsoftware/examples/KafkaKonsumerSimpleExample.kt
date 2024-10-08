package com.eventloopsoftware.examples

import com.eventloopsoftware.consumer.KafkaKonsumer
import com.eventloopsoftware.consumer.KafkaKonsumerParallel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import java.util.*

fun main() {

    val kafkaProperties = Properties().apply {
        this[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
        this[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.qualifiedName
        this[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.qualifiedName
        this[ConsumerConfig.GROUP_ID_CONFIG] = "consumer-example-group-id"
        this[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = true
    }

    // Give Apache to KafkaKonsumer to create a Kotlin FLow consumer
    val kafkaKonsumer = KafkaKonsumer<String, String>(kafkaProperties, setOf("test_topic"))
    val kafkaKonsumerParallel = KafkaKonsumerParallel<String, String>(kafkaProperties, setOf("test_topic"))

    // Run KafkaKonsumer in coroutine
    runApp(kafkaKonsumer)


}

fun runApp(kafkaKonsumer: KafkaKonsumer<String, String>) = runBlocking {
    kafkaKonsumer.startConsume { consumerRecord ->
        delay(5)
        println("key: ${consumerRecord.key()} value: ${consumerRecord.value()}")
    }
}