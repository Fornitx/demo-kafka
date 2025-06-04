package com.example.demokafka.kafka

import com.example.demokafka.AbstractMetricsTest
import com.example.demokafka.kafka.model.DemoRequest
import com.example.demokafka.kafka.model.DemoResponse
import com.example.demokafka.kafka.services.ProduceAndConsumeKafkaService
import com.example.demokafka.utils.log
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.kafka.support.KafkaUtils
import org.springframework.kafka.test.utils.KafkaTestUtils
import java.time.Duration
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

abstract class AbstractKafkaTest : AbstractMetricsTest() {
    protected abstract val bootstrapServers: String

    protected val producerFactory by lazy {
        DefaultKafkaProducerFactory(
            mapOf(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
                ProducerConfig.MAX_BLOCK_MS_CONFIG to 5000,
            ),
            StringSerializer(),
            StringSerializer(),
        )
    }

    private val consumerFactory by lazy {
        DefaultKafkaConsumerFactory(
            mapOf(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG to AbstractKafkaTest::class.simpleName,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ),
            StringDeserializer(),
            StringDeserializer(),
        )
    }

    protected fun produce(
        topic: String,
        data: String,
        headers: Iterable<Header>? = null,
        timeout: Duration = Duration.ofSeconds(5),
    ): RecordMetadata = producerFactory.createProducer().use { producer ->
        val record = ProducerRecord(topic, null, null, null as String?, data, headers)
        producer.send(record).get(timeout.toMillis(), TimeUnit.MILLISECONDS).also { recordMetadata ->
            log.debug { "Sent ${KafkaUtils.format(record)} as $recordMetadata" }
        }
    }

    protected fun consumeSingle(
        topic: String,
        timeout: Duration = Duration.ofSeconds(5),
    ): ConsumerRecord<String, String> = consumerFactory.createConsumer().use { consumer ->
        consumer.subscribe(listOf(topic))
        KafkaTestUtils.getSingleRecord(consumer, topic, timeout)
    }

    protected fun consume(
        topic: String,
        timeout: Duration = Duration.ofSeconds(5),
        minRecords: Int = 1,
    ): ConsumerRecords<String, String> = consumerFactory.createConsumer().use { consumer ->
        consumer.subscribe(listOf(topic))
        KafkaTestUtils.getRecords(consumer, timeout, minRecords)
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////////////////////////

    protected suspend fun CoroutineScope.outInSimpleTest(service: ProduceAndConsumeKafkaService) {
        // call service
        val request = DemoRequest("Abc")
        val deferred = async(Dispatchers.IO) { service.sendAndReceive(request) }

        // consume sent record
        val sentRecord = consumeSingle(properties.kafka.produceConsume.outputTopic)
        log.info { sentRecord.log() }

        log.info { "Sent record: ${KafkaUtils.format(sentRecord)}" }

        // check reply topic header
        val replyTopicHeader = sentRecord.headers().lastHeader(KafkaHeaders.REPLY_TOPIC)
        // TODO reply partition
        assertNotNull(replyTopicHeader)
        assertArrayEquals(
            properties.kafka.produceConsume.inputTopic.encodeToByteArray(),
            replyTopicHeader.value()
        )

        // correlationId topic header
        val correlationIdHeader = sentRecord.headers().lastHeader(KafkaHeaders.CORRELATION_ID)
        assertNotNull(replyTopicHeader)

        val correlationId = correlationIdHeader.value()

        // check record value
        assertEquals(objectMapper.writeValueAsString(request), sentRecord.value())

        // send response
        val response = DemoResponse(request.msg.repeat(3))
        produce(
            properties.kafka.produceConsume.inputTopic,
            objectMapper.writeValueAsString(response),
            listOf(RecordHeader(KafkaHeaders.CORRELATION_ID, correlationId)),
        )

        // check service response
        val serviceResponse = deferred.await()
        assertEquals(response, serviceResponse)
    }
}
