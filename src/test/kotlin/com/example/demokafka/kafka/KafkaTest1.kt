package com.example.demokafka.kafka

import com.example.demokafka.kafka.dto.DemoRequest
import com.example.demokafka.properties.DemoKafkaProperties
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.header.internals.RecordHeaders
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import java.time.Duration
import java.util.*
import kotlin.test.assertEquals

@SpringBootTest
class KafkaTest1 : AbstractKafkaTest() {
    @Autowired
    private lateinit var properties: DemoKafkaProperties

    @Autowired
    private lateinit var objectMapper: ObjectMapper

    @Test
    fun test1() {
        val requestId = UUID.randomUUID().toString()

        log.info { "Sending" }
        val sendResult = produce(
            objectMapper.writeValueAsString(DemoRequest("FOO_")),
            headers = mapOf(RQID to requestId)
        )
        log.info { "Sent $sendResult" }

        val records = consume(properties.kafka.outputTopic)
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
    }

    @Test
    fun test2() {
        val requestId = UUID.randomUUID().toString()

        replyingTemplate.start()
        replyingTemplate.waitForAssignment(Duration.ofSeconds(5))

        log.info { "Sending" }
        val future = replyingTemplate.sendAndReceive(
            ProducerRecord(
                IN_TOPIC, null, null, null,
                objectMapper.writeValueAsString(DemoRequest("FOO_")),
                RecordHeaders(listOf(RecordHeader(RQID, requestId.toByteArray())))
            )
        )
        log.info { "Sent ${future.sendFuture.get()}" }

        val record = future.get()
        log.info { "Received $record" }

        log.info {
            "\nReceived record \n" +
                    "\tkey : ${record.key()}\n" +
                    "\tvalue : ${record.value()}\n" +
                    "\theaders : ${record.headers()}"
        }

        assertEquals(requestId, record.headers().lastHeader(RQID).value().decodeToString())
    }
}
