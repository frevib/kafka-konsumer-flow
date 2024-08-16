package com.eventloopsoftware.consumer

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.MockConsumer
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Test

class KafkaKonsumerParallelTest {

    @Test
    fun `performance test parallel consumer`() = runBlocking {
        // OffsetResetStrategy.EARLIEST -> new consumer group IDs are reading from the beginning of the topic
        val mockConsumer =
            MockConsumer<String, String>(OffsetResetStrategy.EARLIEST)
                .apply {
                    assign(listOf(TopicPartition("my_topic", 0)))
                }
                .apply {
                    val beginningOffsets = mapOf(TopicPartition("my_topic", 0) to 0L)
                    updateBeginningOffsets(beginningOffsets)
                }


        val kafkaConsumerParallel = KafkaKonsumerParallel<String, String>(mockConsumer)

        // These schedulePollTasks are executed right after a call
        // to .poll(). The next schedulePollTasks after the second
        // .poll() etc.
        mockConsumer.schedulePollTask {
            addRecords(mockConsumer, 0, 100_000)
        }
        mockConsumer.schedulePollTask {
            addRecords(mockConsumer, 100_000, 200_000)
        }

        mockConsumer.schedulePollTask { kafkaConsumerParallel.stop() }

        // How to process each Kafka message
        val processFunction: suspend (ConsumerRecord<String, String>) -> Unit = {
            delay(1)
            println("Key: ${it.key()} -- Value: ${it.value()}")
        }

        kafkaConsumerParallel.startConsume(processFunction)
    }

    private fun addRecords(mockConsumer: MockConsumer<String, String>, offsetBegin: Int, offsetEnd: Int) {
        for (i in offsetBegin until offsetEnd) {
            mockConsumer.addRecord(
                ConsumerRecord<String, String>(
                    "my_topic",
                    0, i.toLong(), "key-${i}", "value-${i}"
                )
            )
        }
    }
}