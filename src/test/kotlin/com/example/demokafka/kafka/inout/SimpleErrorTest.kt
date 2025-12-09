package com.example.demokafka.kafka.inout

import com.example.demokafka.TestProfiles
import com.example.demokafka.kafka.AbstractMockKafkaTest
import com.example.demokafka.kafka.metrics.DemoKafkaMetrics
import com.example.demokafka.kafka.metrics.METER_TAG_TOPIC
import com.example.demokafka.kafka.model.DemoRequest
import com.example.demokafka.kafka.model.DemoResponse
import com.example.demokafka.kafka.services.ConsumeAndProduceKafkaService
import com.example.demokafka.utils.Constants
import com.example.demokafka.utils.headers
import kotlinx.coroutines.test.runTest
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.TimestampType
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.stub
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.kafka.support.SendResult
import org.springframework.test.context.ActiveProfiles
import java.util.*
import java.util.concurrent.CompletableFuture

@SpringBootTest
@ActiveProfiles(TestProfiles.CONSUME_PRODUCE)
class SimpleErrorTest : AbstractMockKafkaTest() {
    @Autowired
    private lateinit var service: ConsumeAndProduceKafkaService

    @Test
    fun test() = runTest {
        val consumerTopic = "consumer_topic"
        val producerTopic = "producer_topic"

        (kafkaTemplate as KafkaTemplate<String, DemoResponse>).stub {
            on {
                send(any<ProducerRecord<String, DemoResponse>>())
            } doReturn CompletableFuture.completedFuture(
                SendResult<String, DemoResponse>(
                    ProducerRecord<String, DemoResponse>(producerTopic, null, DemoResponse("producer_value")),
                    RecordMetadata(TopicPartition(producerTopic, 0), 0, -1, -1, -1, -1),
                )
            )
        }

        service.consume(
            ConsumerRecord(
                consumerTopic,
                0,
                0,
                System.currentTimeMillis(),
                TimestampType.CREATE_TIME,
                ConsumerRecord.NULL_SIZE,
                ConsumerRecord.NULL_SIZE,
                null,
                DemoRequest("consumer_value"),
                headers(
                    Constants.RQID to UUID.randomUUID().toString(),
                    KafkaHeaders.REPLY_TOPIC to producerTopic,
                ),
                Optional.empty<Int>(),
                Optional.empty<Short>(),
            )
        )

        assertMeter(DemoKafkaMetrics::kafkaConsumeLag, mapOf(METER_TAG_TOPIC to consumerTopic))
        assertMeter(DemoKafkaMetrics::kafkaConsume, mapOf(METER_TAG_TOPIC to consumerTopic))
        assertMeter(DemoKafkaMetrics::kafkaProduce, mapOf(METER_TAG_TOPIC to producerTopic))
        assertMeter(DemoKafkaMetrics::kafkaTiming, mapOf(METER_TAG_TOPIC to consumerTopic))
    }
}