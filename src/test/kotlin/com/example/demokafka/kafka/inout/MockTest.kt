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
import com.example.demokafka.utils.verifyNoMoreInteractions
import kotlinx.coroutines.test.runTest
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.TimestampType
import org.junit.jupiter.api.Test
import org.mockito.kotlin.*
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.kafka.support.SendResult
import org.springframework.test.context.ActiveProfiles
import java.util.*
import java.util.concurrent.CompletableFuture
import kotlin.test.assertEquals

typealias SR = SendResult<String, DemoResponse>
typealias PR = ProducerRecord<String, DemoResponse>

@SpringBootTest
@ActiveProfiles(TestProfiles.CONSUME_PRODUCE)
class MockTest : AbstractMockKafkaTest() {
    @Autowired
    private lateinit var service: ConsumeAndProduceKafkaService

    private val kafkaTemplate: KafkaTemplate<String, DemoResponse> by lazy { mockedKafkaTemplate as KafkaTemplate<String, DemoResponse> }

    @Test
    fun test() = runTest {
        val consumerTopic = "consumer_topic"
        val producerTopic = "producer_topic"
        val producerPartition = 99

        val sendResult = SR(
            PR(producerTopic, null, DemoResponse("producer_value")),
            RecordMetadata(TopicPartition(producerTopic, 0), 0, -1, -1, -1, -1),
        )
        kafkaTemplate.stub {
            on { send(any<PR>()) } doReturn CompletableFuture.completedFuture(sendResult)
        }

        val consumerRecord = ConsumerRecord<String, DemoRequest>(
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
                KafkaHeaders.REPLY_PARTITION to producerPartition,
            ),
            Optional.empty<Int>(),
            Optional.empty<Short>(),
        )
        service.consume(consumerRecord)

        val argumentCaptor = argumentCaptor<PR>()
        verify(kafkaTemplate).send(argumentCaptor.capture())
        kafkaTemplate.verifyNoMoreInteractions()

        val producerRecord = argumentCaptor.singleValue
        assertEquals(producerTopic, producerRecord.topic())
        assertEquals(producerPartition, producerRecord.partition())
        assertEquals(DemoResponse(consumerRecord.value().msg1.repeat(3)), producerRecord.value())

        assertMeter(DemoKafkaMetrics::kafkaConsumeLag, mapOf(METER_TAG_TOPIC to consumerTopic))
        assertMeter(DemoKafkaMetrics::kafkaConsume, mapOf(METER_TAG_TOPIC to consumerTopic))
        assertMeter(DemoKafkaMetrics::kafkaProduce, mapOf(METER_TAG_TOPIC to producerTopic))
        assertMeter(DemoKafkaMetrics::kafkaTiming, mapOf(METER_TAG_TOPIC to consumerTopic))
    }
}
