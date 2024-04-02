package com.example.demokafka.kafka.testcontainers

import com.example.demokafka.AbstractMetricsTest
import com.example.demokafka.TestProfiles.TESTCONTAINERS
import com.example.demokafka.properties.DemoKafkaProperties
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
import org.springframework.kafka.support.SendResult
import org.springframework.kafka.test.utils.KafkaTestUtils
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import java.time.Duration

@ActiveProfiles(TESTCONTAINERS)
@DirtiesContext
abstract class AbstractTestcontainersKafkaTest : AbstractMetricsTest() {
    protected val kafkaContainer = TestcontainersHelper.kafkaContainer

    @Autowired
    protected lateinit var properties: DemoKafkaProperties

    private val producerFactory by lazy {
        DefaultKafkaProducerFactory(
            mapOf(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaContainer.bootstrapServers),
            StringSerializer(),
            StringSerializer(),
        )
    }

    private val consumerFactory by lazy {
        DefaultKafkaConsumerFactory(
            mapOf(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaContainer.bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG to AbstractTestcontainersKafkaTest::class.simpleName,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ),
            StringDeserializer(),
            StringDeserializer(),
        )
    }

    private val template: KafkaTemplate<String, String> by lazy {
        KafkaTemplate(producerFactory)
    }

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
