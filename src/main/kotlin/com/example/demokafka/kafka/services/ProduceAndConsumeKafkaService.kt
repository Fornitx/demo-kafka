package com.example.demokafka.kafka.services

import com.example.demokafka.kafka.metrics.DemoKafkaMetrics
import com.example.demokafka.kafka.model.DemoRequest
import com.example.demokafka.kafka.model.DemoResponse
import com.example.demokafka.properties.CustomKafkaProperties
import com.example.demokafka.utils.headers
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.future.await
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.KafkaException
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.kafka.support.KafkaUtils
import java.time.Duration

private val log = KotlinLogging.logger {}

class ProduceAndConsumeKafkaService(
    properties: CustomKafkaProperties,
    private val template: ReplyingKafkaTemplate<String, DemoRequest, DemoResponse>,
    private val metrics: DemoKafkaMetrics,
) {
    private val outputTopic = properties.outputTopic

    suspend fun sendAndReceive(request: DemoRequest, timeout: Duration? = null): DemoResponse {
        // TODO blocking call inside coroutine
        template.waitForAssignment(Duration.ofSeconds(10))

        log.info { "Sending demo request $request" }

        val firstTopicPartition = template.assignedReplyTopicPartitions?.firstOrNull()
            ?: throw KafkaException("No assigned topic/partitions found")

        val record = ProducerRecord<String, DemoRequest>(
            outputTopic, null, null, request, headers(
                KafkaHeaders.REPLY_TOPIC to firstTopicPartition.topic(),
                KafkaHeaders.REPLY_PARTITION to firstTopicPartition.partition()
            )
        )
        val requestReplyFuture = when (timeout) {
            null -> template.sendAndReceive(record)
            else -> template.sendAndReceive(record, timeout)
        }

        try {
            val sendResult = requestReplyFuture.sendFuture?.await()
            metrics.kafkaProduce(outputTopic).increment()
            log.info { "Sent ${KafkaUtils.format(record)} as ${sendResult?.recordMetadata}" }
        } catch (ex: Exception) {
            log.error(ex) {}
            metrics.kafkaProduceErrors(outputTopic).increment()
        }

        return requestReplyFuture.await().value()
    }
}
