package com.example.demokafka.kafka

import com.example.demokafka.AbstractMetricsTest
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.support.KafkaUtils
import org.springframework.kafka.test.utils.KafkaTestUtils
import java.time.Duration
import java.util.concurrent.TimeUnit

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
}
