package com.example.demokafka.kafka

import com.example.demokafka.AbstractMetricsTest
import com.example.demokafka.kafka.model.DemoRequest
import com.example.demokafka.kafka.model.DemoResponse
import com.example.demokafka.kafka.services.ProduceAndConsumeKafkaService
import com.example.demokafka.utils.Constants.RQID
import com.example.demokafka.utils.headers
import com.fasterxml.jackson.module.kotlin.readValue
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.kafka.support.KafkaUtils
import org.springframework.kafka.test.utils.KafkaTestUtils
import org.springframework.test.annotation.DirtiesContext
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

@DirtiesContext
abstract class AbstractKafkaTest : AbstractMetricsTest() {
    protected abstract val bootstrapServers: String

    protected val producerFactory by lazy {
        DefaultKafkaProducerFactory(
            mapOf(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers),
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

//    private val template: KafkaTemplate<String, String> by lazy {
//        KafkaTemplate(producerFactory)
//    }

//    protected val replyingTemplate: ReplyingKafkaTemplate<String, String, String> by lazy {
//        ReplyingKafkaTemplate(
//            producerFactory,
//            KafkaMessageListenerContainer(consumerFactory, ContainerProperties(IN_TOPIC).apply {
//                setGroupId(AbstractKafkaTest::class.simpleName!!)
//            })
//        )
//    }

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

    protected fun inOutSimpleTest() {
        val requestId = UUID.randomUUID().toString()

        val sendResult = produce(
            properties.kafka.inOut.inputTopic,
            objectMapper.writeValueAsString(DemoRequest("Abc")),
            headers = headers(
                RQID to requestId,
                KafkaHeaders.REPLY_TOPIC to properties.kafka.inOut.outputTopic,
            )
        )
        log.info { "Sent $sendResult" }

        val records = consume(properties.kafka.inOut.outputTopic)
        assertEquals(1, records.count())

        val record = records.first()
        log.info { "Received $record" }

        log.info {
            "\nReceived record \n" +
                    "\tkey : ${record.key()}\n" +
                    "\tvalue : ${record.value()}\n" +
                    "\theaders : ${record.headers()}"
        }

        assertEquals(requestId, record.headers().lastHeader(RQID).value().decodeToString())
        assertEquals("Abc".repeat(3), objectMapper.readValue<DemoResponse>(record.value()).msg)
    }

    protected fun inOutLoadTest() {
        val count = 100

        producerFactory.createProducer().use { producer ->
            repeat(count) {
                producer.send(
                    ProducerRecord(
                        properties.kafka.inOut.inputTopic,
                        null,
                        null,
                        null,
                        objectMapper.writeValueAsString(DemoRequest("Abc")),
                        RecordHeaders(
                            listOf(
                                RecordHeader(RQID, UUID.randomUUID().toString().toByteArray(Charsets.UTF_8)),
                                RecordHeader(
                                    KafkaHeaders.REPLY_TOPIC,
                                    properties.kafka.inOut.outputTopic.toByteArray(Charsets.UTF_8)
                                ),
                            )
                        ),
                    )
                )
            }
        }

        val records = consume(properties.kafka.inOut.outputTopic, minRecords = count)
        assertEquals(count, records.count())

        assertEquals(count.toDouble(), metrics.kafkaConsume(properties.kafka.inOut.inputTopic).count())
        assertEquals(count.toDouble(), metrics.kafkaProduce(properties.kafka.inOut.outputTopic).count())
    }

    protected suspend fun CoroutineScope.outInSimpleTest(service: ProduceAndConsumeKafkaService) {
        // call service
        val request = DemoRequest("Abc")
        val deferred = async(Dispatchers.IO) { service.sendAndReceive(request) }

        // consume sent record
        val records = consume(properties.kafka.outIn.outputTopic)
        assertEquals(1, records.count())

        val sentRecord = records.first()
        log.info { "Sent record: ${KafkaUtils.format(sentRecord)}" }

        // check reply topic header
        val replyTopicHeader = sentRecord.headers().lastHeader(KafkaHeaders.REPLY_TOPIC)
        assertNotNull(replyTopicHeader)
        assertArrayEquals(
            properties.kafka.outIn.inputTopic.toByteArray(Charsets.UTF_8),
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
            properties.kafka.outIn.inputTopic,
            objectMapper.writeValueAsString(response),
            listOf(RecordHeader(KafkaHeaders.CORRELATION_ID, correlationId)),
        )

        // check service response
        val serviceResponse = deferred.await()
        assertEquals(response, serviceResponse)
    }
}
