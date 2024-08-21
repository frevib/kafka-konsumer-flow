package com.eventloopsoftware.consumer

import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.WakeupException
import java.io.IOException
import java.time.Duration
import java.util.*

private val logger = KotlinLogging.logger {}

class KafkaKonsumerParallel<K, V>(
    val consumer: Consumer<K, V>,
) {

    constructor(
        kafkaConsumerConfig: Properties,
        topics: Set<String>
    ) : this(
        KafkaConsumer<K, V>(kafkaConsumerConfig)
            .apply { subscribe(topics) }
    )


    suspend fun startConsume(processFunction: suspend (ConsumerRecord<K, V>) -> Unit) {
        logger.debug { "Consuming topics: ${consumer.listTopics()}" }

        // This hook is used for when the app receives an interrupt like SIGTERM
        Runtime.getRuntime().addShutdownHook(Thread {
            logger.warn { "Shutdown hook triggered, shutting down KafkaKonsumer..." }
            consumer.wakeup()
        })

        consumer
            .asFlow(processFunction)
            .catch { err ->
                logger.error(err) { "Consuming error: ${err.message}" }
                if (err !is IOException) {
                    logger.error(err) { "Not an IOException: ${err.message}" }
                    throw err
                } // rethrow all but IOException
            }
            .collect()

    }

    private fun <K, V> Consumer<K, V>.asFlow(
        processFunction: suspend (ConsumerRecord<K, V>) -> Unit,
        timeout: Duration = Duration.ofMillis(500),
    ) = channelFlow {
        use { consumer ->
            while (true) {
                val jobs = consumer
                    .poll(timeout)
                    .map { record ->
                        launch {
                            send(processFunction(record))
                        }
                    }
                jobs.joinAll()
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