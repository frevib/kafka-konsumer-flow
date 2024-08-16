package com.eventloopsoftware.consumer

import kotlinx.coroutines.flow.*
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.errors.WakeupException
import java.io.IOException
import java.time.Duration
import java.util.*

import io.github.oshai.kotlinlogging.KotlinLogging

private val logger = KotlinLogging.logger {}

class KafkaKonsumer<K, V>(
    val consumer: org.apache.kafka.clients.consumer.Consumer<K, V>,
) {

    constructor(
        kafkaConsumerConfig: Properties,
        topics: Set<String>
    ) : this(
        consumer = org.apache.kafka.clients.consumer.KafkaConsumer<K, V>(kafkaConsumerConfig).apply {
            subscribe(topics)
        }
    )


    suspend fun startConsume(processFunction: suspend (ConsumerRecord<K, V>) -> Unit) {
        logger.debug { "Consuming topics: ${consumer.listTopics()}" }

        // This hook is used for when the app receives an interrupt like SIGTERM
        Runtime.getRuntime().addShutdownHook(Thread {
            logger.warn { "Shutdown hook triggered, shutting down KafkaKonsumer..." }
            consumer.wakeup()
        })

        consumer
            .asFlow()
            .map(processFunction)
            .catch { err ->
                logger.error(err) { "Consuming error: ${err.message}" }
                if (err !is IOException) {
                    logger.error(err) { "Not an IOException: ${err.message}" }
                    throw err
                } // rethrow all but IOException
            }
            .collect()

    }

    private fun <K, V> org.apache.kafka.clients.consumer.Consumer<K, V>.asFlow(
        timeout: Duration = Duration.ofMillis(
            2000
        )
    ): Flow<ConsumerRecord<K, V>> =
        flow {
            use { consumer ->
                while (true) {
                    consumer
                        .poll(timeout)
                        .forEach { record -> emit(record) }
                }
            }
        }.catch { err ->
            when (err) {
                // we ignore WakeupException, as this is an expected exception
                is WakeupException -> {
                    logger.warn { "Wake up exception, shutting down..." }
                }

                else -> {
                    logger.error(err) { "Kafka consumer error..." }
                    throw err
                }
            }
        }

    // Use to manually stop consumer
    fun stop() = consumer.wakeup()

}