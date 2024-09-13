package com.example.demokafka.kafka.testcontainers.tcnew.inout

import com.example.demokafka.TestProfiles
import com.example.demokafka.kafka.model.DemoRequest
import com.example.demokafka.kafka.model.DemoResponse
import com.example.demokafka.kafka.testcontainers.tcnew.AbstractNewTestcontainersKafkaTest
import com.example.demokafka.utils.Constants.RQID
import com.example.demokafka.utils.headers
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.junit.jupiter.api.RepeatedTest
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.test.context.ActiveProfiles
import java.util.*
import kotlin.test.assertEquals

@SpringBootTest
@ActiveProfiles(TestProfiles.IN_OUT)
class NewRepeated1Test : AbstractNewTestcontainersKafkaTest() {
    @Autowired
    private lateinit var objectMapper: ObjectMapper

    @RepeatedTest(5)
    fun test() {
        val requestId = UUID.randomUUID().toString()

        val sendResult = produce(
            properties.inOutKafka.inputTopic,
            objectMapper.writeValueAsString(DemoRequest("Abc")),
            headers = headers(
                RQID to requestId,
                KafkaHeaders.REPLY_TOPIC to properties.inOutKafka.outputTopic,
            )
        )
        log.info { "Sent $sendResult" }

        val records = consume(properties.inOutKafka.outputTopic)
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
}
