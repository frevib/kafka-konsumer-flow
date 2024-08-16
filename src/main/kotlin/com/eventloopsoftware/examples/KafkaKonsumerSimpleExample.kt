package com.eventloopsoftware.examples

import com.eventloopsoftware.consumer.KafkaKonsumer
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
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

    // Create Apache Consumer
    val consumer = KafkaConsumer<String, String>(kafkaProperties).apply {
        subscribe(setOf("test_topic"))
    }

    // Give Apache to KafkaKonsumer to create a Kotlin FLow consumer
    val kafkaKonsumer = KafkaKonsumer(consumer)

    // Run KafkaKonsumer in coroutine
    runApp(kafkaKonsumer)


}

fun runApp(kafkaKonsumer: KafkaKonsumer<String, String>) = runBlocking {
    kafkaKonsumer.startConsume { consumerRecord ->
        delay(5)
        println("key: ${consumerRecord.key()} value: ${consumerRecord.value()}")
    }
}