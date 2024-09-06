package com.example.demokafka.kafka.services

import com.example.demokafka.kafka.metrics.DemoKafkaMetrics
import com.example.demokafka.kafka.model.DemoRequest
import com.example.demokafka.kafka.model.DemoResponse
import com.example.demokafka.properties.DemoKafkaProperties
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.future.await
import org.apache.kafka.clients.producer.ProducerRecord
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate

private val log = KotlinLogging.logger {}

class ProduceAndConsumeKafkaService(
    private val properties: DemoKafkaProperties,
    private val template: ReplyingKafkaTemplate<String, DemoRequest, DemoResponse>,
    private val metrics: DemoKafkaMetrics,
) {
    suspend fun sendAndAwait(request: DemoRequest): DemoResponse {
        val topic = properties.outInKafka.outputTopic
        val requestReplyFuture = template.sendAndReceive(ProducerRecord(topic, request))

        try {
            val sendResult = requestReplyFuture.sendFuture.await()
            metrics.kafkaProduce(topic).increment()
        } catch (ex: Exception) {
            metrics.kafkaProduceErrors(topic).increment()
        }

        return requestReplyFuture.await().value()
    }
}
