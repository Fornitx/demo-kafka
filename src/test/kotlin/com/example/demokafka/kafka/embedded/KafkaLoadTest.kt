package com.example.demokafka.kafka.embedded

import com.example.demokafka.kafka.model.DemoRequest
import com.example.demokafka.utils.Constants.RQID
import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.RepeatedTest
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.support.KafkaHeaders
import java.util.*
import kotlin.test.assertEquals

@SpringBootTest
internal class KafkaLoadTest : AbstractEmbeddedKafkaTest() {

    @Autowired
    private lateinit var objectMapper: ObjectMapper

    @RepeatedTest(5)
    fun test() {
        val requestId = UUID.randomUUID().toString()

        repeat(100) {
            produce(
                properties.kafka.inputTopic,
                objectMapper.writeValueAsString(DemoRequest("Abc")),
                headers = mapOf(
                    RQID to requestId,
                    KafkaHeaders.REPLY_TOPIC to properties.kafka.outputTopic,
                )
            )
        }

        val records = consume(properties.kafka.outputTopic, minRecords = 100)
        assertEquals(100, records.count())

        assertEquals(100.0, metrics.kafkaConsume(properties.kafka.inputTopic).count())
        assertEquals(100.0, metrics.kafkaProduce(properties.kafka.outputTopic).count())
    }
}
