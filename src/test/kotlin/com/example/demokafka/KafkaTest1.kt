package com.example.demokafka

import com.example.demokafka.kafka.DemoKafkaProperties
import com.example.demokafka.kafka.dto.DemoRequest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import java.time.Duration

@SpringBootTest
class KafkaTest1 : BaseKafkaTest() {
    @Autowired
    private lateinit var properties: DemoKafkaProperties

    @Test
    fun test() {
        log.info { "Sending" }
        val sendResult = template.send(properties.kafka.inputTopic, DemoRequest("FOO_")).get()
        log.info { "Sent $sendResult" }

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
