package com.example.demokafka.kafka.testcontainers

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
internal class KafkaLoadTest : AbstractTestcontainersKafkaTest() {

    @Autowired
    private lateinit var objectMapper: ObjectMapper

    @RepeatedTest(10)
    fun test() {
        val requestId = UUID.randomUUID().toString()

        repeat(100) {
            produce(
                IN_TC_TOPIC,
                objectMapper.writeValueAsString(DemoRequest("Abc")),
                headers = mapOf(
                    RQID to requestId,
                    KafkaHeaders.REPLY_TOPIC to OUT_TC_TOPIC,
                )
            )
        }

        val records = consume(OUT_TC_TOPIC, minRecords = 100)
        assertEquals(100, records.count())

        assertEquals(100.0, metrics.kafkaConsume(IN_TC_TOPIC).count())
        assertEquals(100.0, metrics.kafkaProduce(OUT_TC_TOPIC).count())
    }
}
