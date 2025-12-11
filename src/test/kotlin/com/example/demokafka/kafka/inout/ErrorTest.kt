package com.example.demokafka.kafka.inout

import com.example.demokafka.TestProfiles
import com.example.demokafka.kafka.AbstractTestcontainersKafkaTest
import com.example.demokafka.kafka.metrics.DemoKafkaMetrics
import com.example.demokafka.kafka.metrics.METER_TAG_TOPIC
import com.example.demokafka.properties.CustomKafkaProperties
import com.example.demokafka.utils.Constants.RQID
import com.example.demokafka.utils.headers
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.test.context.ActiveProfiles
import java.util.UUID.randomUUID
import java.util.concurrent.TimeUnit

@SpringBootTest
@ActiveProfiles(TestProfiles.CONSUME_PRODUCE)
@Disabled // TODO
class ErrorTest : AbstractTestcontainersKafkaTest() {
    private val kafkaProps: CustomKafkaProperties
        get() = properties.kafka.consumeProduce

    @Test
    fun testDeserialization() {
        val requestId = randomUUID().toString()
        val sendResult = produce(
            kafkaProps.inputTopic,
            "{}}",
            headers = headers(
                RQID to requestId,
                KafkaHeaders.REPLY_TOPIC to kafkaProps.outputTopic,
            )
        )
        log.info { "Sent $sendResult" }

//        val record = consumeSingle(kafkaProps.outputTopic)
//        log.info { record.log() }

//        assertEquals(partition, record.partition())
//        assertEquals(requestId, record.headers().lastHeader(RQID).value().decodeToString())
//        assertEquals("Abc".repeat(3), jsonMapper.readValue<DemoResponse>(record.value()).msg2)

        TimeUnit.SECONDS.sleep(5)

        assertMeter(DemoKafkaMetrics::kafkaConsumeLag, mapOf(METER_TAG_TOPIC to kafkaProps.inputTopic))
        assertMeter(DemoKafkaMetrics::kafkaConsume, mapOf(METER_TAG_TOPIC to kafkaProps.inputTopic))
        assertNoMeter(DemoKafkaMetrics::kafkaProduce)
        assertNoMeter(DemoKafkaMetrics::kafkaTiming)
    }
}
