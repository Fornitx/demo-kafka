package com.example.demokafka.kafka.testcontainers

import com.example.demokafka.kafka.model.DemoRequest
import com.example.demokafka.kafka.model.DemoResponse
import com.example.demokafka.utils.Constants.RQID
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.junit.jupiter.api.RepeatedTest
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.support.KafkaHeaders
import java.util.*
import kotlin.test.assertEquals

@SpringBootTest
internal class KafkaRepeated2Test : AbstractTestcontainersKafkaTest() {
    @Autowired
    private lateinit var objectMapper: ObjectMapper

    @RepeatedTest(10)
    fun test() {
        val requestId = UUID.randomUUID().toString()

        val sendResult = produce(
            IN_TC_TOPIC,
            objectMapper.writeValueAsString(DemoRequest("Abc")),
            headers = mapOf(
                RQID to requestId,
                KafkaHeaders.REPLY_TOPIC to OUT_TC_TOPIC,
            )
        )
        log.info { "Sent $sendResult" }

        val records = consume(OUT_TC_TOPIC)
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

//    @Test
//    fun test2() {
//        val requestId = UUID.randomUUID().toString()
//
//        replyingTemplate.start()
//        replyingTemplate.waitForAssignment(Duration.ofSeconds(5))
//
//        log.info { "Sending" }
//        val future = replyingTemplate.sendAndReceive(
//            ProducerRecord(
//                IN_TOPIC, null, null, null,
//                objectMapper.writeValueAsString(DemoRequest("FOO_")),
//                RecordHeaders(listOf(RecordHeader(RQID, requestId.toByteArray())))
//            )
//        )
//        log.info { "Sent ${future.sendFuture.get()}" }
//
//        val record = future.get()
//        log.info { "Received $record" }
//
//        log.info {
//            "\nReceived record \n" +
//                    "\tkey : ${record.key()}\n" +
//                    "\tvalue : ${record.value()}\n" +
//                    "\theaders : ${record.headers()}"
//        }
//
//        assertEquals(requestId, record.headers().lastHeader(RQID).value().decodeToString())
//    }
}
