package com.example.demokafka

import com.example.demokafka.kafka.DemoKafkaProperties
import com.example.demokafka.kafka.dto.DemoRequest
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.TopicPartitionOffset
import org.springframework.kafka.support.serializer.JsonDeserializer
import org.springframework.kafka.support.serializer.JsonSerializer
import java.util.concurrent.TimeUnit

@SpringBootTest
class KafkaTest : BaseKafkaTest() {
    @Autowired
    private lateinit var properties: DemoKafkaProperties

    @Test
    fun contextLoads() {
        val template = KafkaTemplate(
            DefaultKafkaProducerFactory(
                mapOf(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaContainer.bootstrapServers),
                StringSerializer(),
                JsonSerializer<DemoRequest>()
            )
        )
        template.setConsumerFactory(
            DefaultKafkaConsumerFactory(
                mapOf(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaContainer.bootstrapServers
                ), StringDeserializer(), JsonDeserializer<DemoRequest>().trustedPackages("*")
            )
        )

        TimeUnit.SECONDS.sleep(2)
        log.info { "Sending" }
        template.send(properties.kafka.inputTopic, DemoRequest("FOO_"))
        log.info { "Sent" }

        val partitions = template.partitionsFor(properties.kafka.outputTopic)
        log.info { "partitions: $partitions" }

        val received = template.receive(partitions.map { TopicPartitionOffset(it.topic(), it.partition(), 0) })
        log.info { "received ${received.count()} : $received" }

        val record = received.records(properties.kafka.outputTopic).iterator().next()
        log.info { "record : $record" }
    }
}
