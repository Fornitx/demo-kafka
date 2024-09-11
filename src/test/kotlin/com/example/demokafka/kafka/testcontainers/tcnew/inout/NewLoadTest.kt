package com.example.demokafka.kafka.testcontainers.tcnew.inout

import com.example.demokafka.TestProfiles
import com.example.demokafka.kafka.model.DemoRequest
import com.example.demokafka.kafka.testcontainers.tcnew.AbstractNewTestcontainersKafkaTest
import com.example.demokafka.utils.Constants.RQID
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.header.internals.RecordHeaders
import org.junit.jupiter.api.RepeatedTest
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.test.context.ActiveProfiles
import java.util.*
import kotlin.test.assertEquals

@SpringBootTest
@ActiveProfiles(TestProfiles.IN_OUT)
class NewLoadTest : AbstractNewTestcontainersKafkaTest() {
    @Autowired
    private lateinit var objectMapper: ObjectMapper

    @RepeatedTest(5)
    fun test() {
        val requestId = UUID.randomUUID().toString()

        producerFactory.createProducer().use { producer ->
            repeat(100) {
                producer.send(
                    ProducerRecord(
                        properties.inOutKafka.inputTopic,
                        null,
                        null,
                        null,
                        objectMapper.writeValueAsString(DemoRequest("Abc")),
                        RecordHeaders(
                            listOf(
                                RecordHeader(RQID, requestId.toByteArray(Charsets.UTF_8)),
                                RecordHeader(
                                    KafkaHeaders.REPLY_TOPIC,
                                    properties.inOutKafka.outputTopic.toByteArray(Charsets.UTF_8)
                                ),
                            )
                        ),
                    )
                )
            }
        }

        val records = consume(properties.inOutKafka.outputTopic, minRecords = 100)
        assertEquals(100, records.count())

        assertEquals(100.0, metrics.kafkaConsume(properties.inOutKafka.inputTopic).count())
        assertEquals(100.0, metrics.kafkaProduce(properties.inOutKafka.outputTopic).count())
    }
}
