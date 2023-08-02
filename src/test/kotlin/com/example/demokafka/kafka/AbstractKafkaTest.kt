package com.example.demokafka.kafka

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.listener.KafkaMessageListenerContainer
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate
import org.springframework.kafka.support.SendResult
import org.springframework.kafka.test.EmbeddedKafkaBroker
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.kafka.test.utils.KafkaTestUtils
import org.springframework.test.annotation.DirtiesContext
import java.time.Duration
import kotlin.reflect.jvm.jvmName

const val IN_TOPIC = "in_topic"
const val OUT_TOPIC = "out_topic"

@EmbeddedKafka(partitions = 1, topics = [IN_TOPIC, OUT_TOPIC])
@DirtiesContext
abstract class AbstractKafkaTest {
    @Autowired
    protected lateinit var embeddedKafkaBroker: EmbeddedKafkaBroker

    protected val log = KotlinLogging.logger(this::class.jvmName)

    private val producerFactory by lazy {
        DefaultKafkaProducerFactory(
            mapOf(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to embeddedKafkaBroker.brokersAsString),
            StringSerializer(),
            StringSerializer(),
        )
    }

    private val consumerFactory by lazy {
        DefaultKafkaConsumerFactory(
            mapOf(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to embeddedKafkaBroker.brokersAsString,
                ConsumerConfig.GROUP_ID_CONFIG to AbstractKafkaTest::class.simpleName,
//                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ),
            StringDeserializer(),
            StringDeserializer(),
        )
    }

    private val template: KafkaTemplate<String, String> by lazy {
        KafkaTemplate(producerFactory)
    }

    protected val replyingTemplate: ReplyingKafkaTemplate<String, String, String> by lazy {
        ReplyingKafkaTemplate(
            producerFactory,
            KafkaMessageListenerContainer(consumerFactory, ContainerProperties(IN_TOPIC).apply {
                setGroupId(AbstractKafkaTest::class.simpleName!!)
            })
        )
    }

    protected fun produce(
        data: String,
        topic: String = IN_TOPIC,
        headers: Map<String, String>? = null,
    ): SendResult<String, String> = template.send(
        ProducerRecord(topic, null, null, null, data, headers?.let {
            RecordHeaders(it.map { (key, value) -> RecordHeader(key, value.toByteArray()) })
        })
    ).get()

    protected fun consume(
        topic: String,
        timeout: Duration = Duration.ofSeconds(5),
        minRecords: Int = 1,
    ): ConsumerRecords<String, String> = consumerFactory.createConsumer().use { consumer ->
        consumer.subscribe(listOf(topic))
        KafkaTestUtils.getRecords(consumer, timeout, minRecords)
    }
}
