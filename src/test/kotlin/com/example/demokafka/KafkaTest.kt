package com.example.demokafka

import com.example.demokafka.kafka.DemoKafkaProperties
import com.example.demokafka.kafka.dto.DemoRequest
import com.example.demokafka.kafka.dto.DemoResponse
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.serializer.JsonDeserializer
import org.springframework.kafka.support.serializer.JsonSerializer
import java.time.Duration

@SpringBootTest
class KafkaTest : BaseKafkaTest() {
    @Autowired
    private lateinit var properties: DemoKafkaProperties

    @Test
    fun test() {
        val template = KafkaTemplate(
            DefaultKafkaProducerFactory(
                mapOf(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaContainer.bootstrapServers),
                StringSerializer(),
                JsonSerializer<DemoRequest>()
            )
        )
        val consumerFactory = DefaultKafkaConsumerFactory(
            mapOf(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaContainer.bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG to this::class.java.simpleName,
            ),
            StringDeserializer(),
            JsonDeserializer<DemoResponse>().trustedPackages("*")
        )

        log.info { "Sending" }
        template.send(properties.kafka.inputTopic, DemoRequest("FOO_"))
        log.info { "Sent" }

        val records = consume(consumerFactory, properties.kafka.outputTopic, Duration.ofSeconds(5), 2)
        assertThat(records.count()).isEqualTo(2)

        val (first, second) = records.records(properties.kafka.outputTopic).toList()

        log.info {
            "\nRecord 1\n" +
                "\tkey : ${first.key()}\n" +
                "\tvalue : ${first.value()}\n" +
                "\theaders : ${first.headers()}"
        }
        log.info {
            "\nRecord 2\n" +
                "\tkey : ${second.key()}\n" +
                "\tvalue : ${second.value()}\n" +
                "\theaders : ${second.headers()}"
        }
    }
}
