package com.example.demokafka.kafka.embedded.inout

import com.example.demokafka.TestProfiles
import com.example.demokafka.kafka.embedded.AbstractEmbeddedKafkaTest
import com.example.demokafka.kafka.model.DemoRequest
import com.example.demokafka.utils.Constants.RQID
import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.RepeatedTest
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.test.context.ActiveProfiles
import java.util.*
import kotlin.test.assertEquals

@SpringBootTest
@ActiveProfiles(TestProfiles.IN_OUT)
class EmbLoadTest : AbstractEmbeddedKafkaTest() {
    @Autowired
    private lateinit var objectMapper: ObjectMapper

    @RepeatedTest(5)
    fun test() {
        val requestId = UUID.randomUUID().toString()

        repeat(100) {
            produce(
                properties.inOutKafka.inputTopic,
                objectMapper.writeValueAsString(DemoRequest("Abc")),
                headers = mapOf(
                    RQID to requestId,
                    KafkaHeaders.REPLY_TOPIC to properties.inOutKafka.outputTopic,
                )
            )
        }

        val records = consume(properties.inOutKafka.outputTopic, minRecords = 100)
        assertEquals(100, records.count())

        assertEquals(100.0, metrics.kafkaConsume(properties.inOutKafka.inputTopic).count())
        assertEquals(100.0, metrics.kafkaProduce(properties.inOutKafka.outputTopic).count())
    }
}
