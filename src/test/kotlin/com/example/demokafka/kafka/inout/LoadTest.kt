package com.example.demokafka.kafka.inout

import com.example.demokafka.TestProfiles
import com.example.demokafka.kafka.AbstractTestcontainersKafkaTest
import com.example.demokafka.kafka.metrics.DemoKafkaMetrics
import com.example.demokafka.kafka.metrics.METER_TAG_TOPIC
import com.example.demokafka.kafka.model.DemoRequest
import com.example.demokafka.properties.CustomKafkaProperties
import com.example.demokafka.utils.Constants.RQID
import com.example.demokafka.utils.headers
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.utils.CircularIterator
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.test.context.ActiveProfiles
import java.util.UUID.randomUUID
import kotlin.test.assertEquals

@SpringBootTest
@ActiveProfiles(TestProfiles.CONSUME_PRODUCE)
class LoadTest : AbstractTestcontainersKafkaTest() {
    private val kafkaProps: CustomKafkaProperties
        get() = properties.kafka.consumeProduce

    @Test
    fun test() {
        val count = 100
        val partitionIterator = CircularIterator(listOf(0, 1))
        producerFactory.createProducer().use { producer ->
            repeat(count) {
                producer.send(
                    ProducerRecord(
                        kafkaProps.inputTopic,
                        null,
                        null,
                        null,
                        jsonMapper.writeValueAsString(DemoRequest("Abc")),
                        RecordHeaders(
                            headers(
                                RQID to randomUUID().toString(),
//                                KafkaHeaders.REPLY_TOPIC to kafkaProps.outputTopic,
                                KafkaHeaders.REPLY_PARTITION to partitionIterator.next(),
                            ),
                        ),
                    )
                )
            }
            producer.flush()
        }
        val records = consume(kafkaProps.outputTopic, minRecords = count)
        assertEquals(count, records.count())

        assertThat(records.records(TopicPartition(kafkaProps.outputTopic, 0))).hasSize(50)
        assertThat(records.records(TopicPartition(kafkaProps.outputTopic, 1))).hasSize(50)

        assertMeter(DemoKafkaMetrics::kafkaConsumeLag, mapOf(METER_TAG_TOPIC to kafkaProps.inputTopic), count)
        assertMeter(DemoKafkaMetrics::kafkaConsume, mapOf(METER_TAG_TOPIC to kafkaProps.inputTopic), count)
        assertMeter(DemoKafkaMetrics::kafkaProduce, mapOf(METER_TAG_TOPIC to kafkaProps.outputTopic), count)
    }
}
