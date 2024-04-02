package com.example.demokafka.kafka.load

import com.example.demokafka.AbstractTest
import com.example.demokafka.kafka.model.DemoRequest
import com.example.demokafka.kafka.testcontainers.TestcontainersHelper
import com.example.demokafka.utils.Constants.RQID
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Test
import org.springframework.kafka.config.TopicBuilder
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaAdmin
import org.springframework.kafka.support.KafkaHeaders
import java.util.*
import kotlin.random.Random

private const val IN_TOPIC = "in_topic"
private const val OUT_TOPIC = "out_topic"
private const val PARTITIONS = 4
private const val N = 20

class KafkaLoadTest : AbstractTest() {
    private val kafkaContainer = TestcontainersHelper.kafkaContainer

    @Test
    fun test() {
        val objectMapper = jacksonObjectMapper()

        val kafkaAdmin =
            KafkaAdmin(mapOf(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaContainer.bootstrapServers))
        kafkaAdmin.createOrModifyTopics(TopicBuilder.name(IN_TOPIC).partitions(PARTITIONS).build())

        DefaultKafkaProducerFactory<String, String>(
            mapOf(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaContainer.bootstrapServers)
        ).apply {
            keySerializer = StringSerializer()
            valueSerializer = StringSerializer()
        }.createProducer().use { producer ->
            repeat(N) {
                val partition = Random.nextInt(0, PARTITIONS)
                val value = objectMapper.writeValueAsString(DemoRequest("$it"))
                val headers = RecordHeaders()
                    .add(RecordHeader(RQID, UUID.randomUUID().toString().toByteArray()))
                    .add(RecordHeader(KafkaHeaders.REPLY_TOPIC, OUT_TOPIC.toByteArray()))
                val record = ProducerRecord<String, String>(IN_TOPIC, partition, null, value, headers)
                producer.send(record).get()
            }
        }
    }
}
