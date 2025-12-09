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
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.test.context.ActiveProfiles
import tools.jackson.module.kotlin.readValue
import java.util.UUID.randomUUID
import kotlin.test.assertEquals

@SpringBootTest
@ActiveProfiles(TestProfiles.CONSUME_PRODUCE)
class SimpleTest : AbstractTestcontainersKafkaTest() {
    private val kafkaProps: CustomKafkaProperties
        get() = properties.kafka.consumeProduce

    @ParameterizedTest
    @ValueSource(ints = [0, 1])
    fun test(partition: Int) {
        val requestId = randomUUID().toString()
        val sendResult = produce(
            kafkaProps.inputTopic,
            jsonMapper.writeValueAsString(DemoRequest("Abc")),
            headers = headers(
                RQID to requestId,
                KafkaHeaders.REPLY_TOPIC to kafkaProps.outputTopic,
                KafkaHeaders.REPLY_PARTITION to partition,
            )
        )
        log.info { "Sent $sendResult" }

        val record = consumeSingle(kafkaProps.outputTopic)
        log.info { record.log() }

        assertEquals(partition, record.partition())
        assertEquals(requestId, record.headers().lastHeader(RQID).value().decodeToString())
        assertEquals("Abc".repeat(3), jsonMapper.readValue<DemoResponse>(record.value()).msg2)

        assertMeter(DemoKafkaMetrics::kafkaConsumeLag, mapOf(METER_TAG_TOPIC to kafkaProps.inputTopic))
        assertMeter(DemoKafkaMetrics::kafkaConsume, mapOf(METER_TAG_TOPIC to kafkaProps.inputTopic))
        assertMeter(DemoKafkaMetrics::kafkaProduce, mapOf(METER_TAG_TOPIC to kafkaProps.outputTopic))
        assertMeter(DemoKafkaMetrics::kafkaTiming, mapOf(METER_TAG_TOPIC to kafkaProps.inputTopic))
    }
}
