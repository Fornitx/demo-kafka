package com.example.demokafka.kafka.inout

import com.example.demokafka.TestProfiles
import com.example.demokafka.kafka.AbstractTestcontainersKafkaTest
import com.example.demokafka.kafka.metrics.DemoKafkaMetrics
import com.example.demokafka.kafka.metrics.METER_TAG_TOPIC
import com.example.demokafka.kafka.model.DemoRequest
import com.example.demokafka.kafka.model.DemoResponse
import com.example.demokafka.properties.CustomKafkaProperties
import com.example.demokafka.utils.Constants.RQID
import com.example.demokafka.utils.headers
import com.example.demokafka.utils.log
import com.fasterxml.jackson.module.kotlin.readValue
import org.junit.jupiter.api.RepeatedTest
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.test.context.ActiveProfiles
import java.util.UUID.randomUUID
import kotlin.test.assertEquals

@SpringBootTest
@ActiveProfiles(TestProfiles.CONSUME_PRODUCE)
class SimpleTest : AbstractTestcontainersKafkaTest() {
    private val kafkaProps: CustomKafkaProperties
        get() = properties.kafka.consumeProduce

    @RepeatedTest(5)
    fun test() {
        val requestId = randomUUID().toString()
        val sendResult = produce(
            kafkaProps.inputTopic,
            objectMapper.writeValueAsString(DemoRequest("Abc")),
            headers = headers(
                RQID to requestId,
                KafkaHeaders.REPLY_TOPIC to kafkaProps.outputTopic,
                KafkaHeaders.REPLY_PARTITION to 0,
            )
        )
        log.info { "Sent $sendResult" }
        val record = consumeSingle(kafkaProps.outputTopic)
        log.info { record.log() }
        assertEquals(requestId, record.headers().lastHeader(RQID).value().decodeToString())
        assertEquals("Abc".repeat(3), objectMapper.readValue<DemoResponse>(record.value()).msg)

        assertMeter(DemoKafkaMetrics::kafkaConsume, mapOf(METER_TAG_TOPIC to kafkaProps.inputTopic))
        assertMeter(DemoKafkaMetrics::kafkaProduce, mapOf(METER_TAG_TOPIC to kafkaProps.outputTopic))
    }
}
