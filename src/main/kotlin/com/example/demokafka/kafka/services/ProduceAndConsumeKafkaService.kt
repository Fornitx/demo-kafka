package com.example.demokafka.kafka.services

import com.example.demokafka.kafka.ReactiveReplyingKafkaTemplate
import com.example.demokafka.kafka.metrics.DemoKafkaMetrics
import com.example.demokafka.kafka.model.DemoRequest
import com.example.demokafka.kafka.model.DemoResponse
import com.example.demokafka.properties.DemoKafkaKafkaProperties
import com.example.demokafka.properties.DemoKafkaProperties
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.future.await
import org.apache.kafka.clients.producer.ProducerRecord
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate
import org.springframework.kafka.support.KafkaUtils
import java.time.Duration

private val log = KotlinLogging.logger {}

sealed class ProduceAndConsumeKafkaService(
    private val properties: DemoKafkaKafkaProperties,
) {
    protected val topic: String
        get() = properties.outIn.outputTopic

    abstract suspend fun sendAndReceive(request: DemoRequest, timeout: Duration? = null): DemoResponse
}

class ProduceAndConsumeKafkaServiceOldImpl(
    properties: DemoKafkaProperties,
    private val template: ReplyingKafkaTemplate<String, DemoRequest, DemoResponse>,
    private val metrics: DemoKafkaMetrics,
) : ProduceAndConsumeKafkaService(properties.kafka) {
    override suspend fun sendAndReceive(request: DemoRequest, timeout: Duration?): DemoResponse {
        log.info { "Sending demo request $request" }

        val record = ProducerRecord<String, DemoRequest>(topic, request)
        val requestReplyFuture = when (timeout) {
            null -> template.sendAndReceive(record)
            else -> template.sendAndReceive(record, timeout)
        }

        try {
            val sendResult = requestReplyFuture.sendFuture.await()
            metrics.kafkaProduce(topic).increment()
            log.info { "Sent ${KafkaUtils.format(record)} as ${sendResult.recordMetadata}" }
        } catch (ex: Exception) {
            log.error(ex) {}
            metrics.kafkaProduceErrors(topic).increment()
        }

        return requestReplyFuture.await().value()
    }
}

class ProduceAndConsumeKafkaServiceNewImpl(
    properties: DemoKafkaProperties,
    private val template: ReactiveReplyingKafkaTemplate<String, DemoRequest, DemoResponse>,
) : ProduceAndConsumeKafkaService(properties.kafka) {
    override suspend fun sendAndReceive(request: DemoRequest, timeout: Duration?): DemoResponse {
        log.info { "Sending demo request $request" }

        val producerRecord = ProducerRecord<String, DemoRequest>(topic, request)
        val consumerRecord = when (timeout) {
            null -> template.sendAndReceive(producerRecord)
            else -> template.sendAndReceive(producerRecord, timeout)
        }
        return consumerRecord.value()
    }
}
