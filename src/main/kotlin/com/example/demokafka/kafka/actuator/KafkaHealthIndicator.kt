package com.example.demokafka.kafka.actuator

import com.example.demokafka.properties.KafkaHealthProperties
import org.apache.kafka.clients.producer.RecordMetadata
import org.springframework.boot.health.contributor.Health
import org.springframework.boot.health.contributor.HealthIndicator
import org.springframework.kafka.core.KafkaTemplate

class KafkaHealthIndicator(
    properties: KafkaHealthProperties,
    private val kafkaTemplate: KafkaTemplate<String, Long>,
) : HealthIndicator {
    private val topic = properties.outputTopic

    override fun health(): Health {
        val currentTimeMillis = System.currentTimeMillis()
        return try {
            val sendResult = kafkaTemplate.send(topic, currentTimeMillis).get()
            Health.up()
                .withTopic(topic)
                .withMillis(currentTimeMillis)
                .withMeta(sendResult.recordMetadata)
                .build()
        } catch (ex: Exception) {
            Health.down()
                .withTopic(topic)
                .withMillis(currentTimeMillis)
                .withError(ex)
                .build()
        }
    }

    private fun Health.Builder.withTopic(topic: String): Health.Builder =
        this.withDetail("topic", topic)

    private fun Health.Builder.withMillis(millis: Long): Health.Builder =
        this.withDetail("currentTimeMillis", millis)

    private fun Health.Builder.withMeta(meta: RecordMetadata): Health.Builder =
        this.withDetail("partition", meta.partition())
            .withDetail("offset", meta.offset())

    private fun Health.Builder.withError(ex: Exception): Health.Builder =
        this.withDetail("error", ex.message.toString())
}
