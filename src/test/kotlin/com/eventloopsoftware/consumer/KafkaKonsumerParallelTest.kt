package com.eventloopsoftware.consumer

import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.MockConsumer
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Test

class KafkaKonsumerParallelTest {

    @Test
    fun `performance test parallel consumer`() = runTest {
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

        mockConsumer.schedulePollTask {
            addRecords(mockConsumer, 0, 1_000)
        }
        mockConsumer.schedulePollTask {
            addRecords(mockConsumer, 1_000, 2_000)
        }

        mockConsumer.schedulePollTask { kafkaConsumerParallel.stop() }

        val processRecordFunction: suspend (ConsumerRecord<String, String>) -> Unit = {
            delay(1)
            println("Key: ${it.key()} -- Value: ${it.value()}")
        }

        kafkaConsumerParallel.startConsume(processRecordFunction)
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