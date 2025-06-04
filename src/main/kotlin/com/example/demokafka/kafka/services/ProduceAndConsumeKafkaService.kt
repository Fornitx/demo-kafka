package com.example.demokafka.kafka.services

import com.example.demokafka.kafka.ReactiveReplyingKafkaTemplate
import com.example.demokafka.kafka.metrics.DemoKafkaMetrics
import com.example.demokafka.kafka.model.DemoRequest
import com.example.demokafka.kafka.model.DemoResponse
import com.example.demokafka.properties.DemoKafkaProperties
import com.example.demokafka.properties.DemoProperties
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.future.await
import org.apache.kafka.clients.producer.ProducerRecord
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate
import org.springframework.kafka.support.KafkaUtils
import java.time.Duration

private val log = KotlinLogging.logger {}

sealed class ProduceAndConsumeKafkaService(
    properties: DemoKafkaProperties,
) {
    protected val outputTopic = properties.produceConsume.outputTopic

    abstract suspend fun sendAndReceive(request: DemoRequest, timeout: Duration? = null): DemoResponse
}

class ProduceAndConsumeKafkaServiceOldImpl(
    properties: DemoProperties,
    private val template: ReplyingKafkaTemplate<String, DemoRequest, DemoResponse>,
    private val metrics: DemoKafkaMetrics,
) : ProduceAndConsumeKafkaService(properties.kafka) {
    override suspend fun sendAndReceive(request: DemoRequest, timeout: Duration?): DemoResponse {
        log.info { "Sending demo request $request" }

        val record = ProducerRecord<String, DemoRequest>(outputTopic, request)
        val requestReplyFuture = when (timeout) {
            null -> template.sendAndReceive(record)
            else -> template.sendAndReceive(record, timeout)
        }

        try {
            val sendResult = requestReplyFuture.sendFuture.await()
            metrics.kafkaProduce(outputTopic).increment()
            log.info { "Sent ${KafkaUtils.format(record)} as ${sendResult.recordMetadata}" }
        } catch (ex: Exception) {
            log.error(ex) {}
            metrics.kafkaProduceErrors(outputTopic).increment()
        }

        return requestReplyFuture.await().value()
    }
}

class ProduceAndConsumeKafkaServiceNewImpl(
    properties: DemoProperties,
    private val template: ReactiveReplyingKafkaTemplate<String, DemoRequest, DemoResponse>,
) : ProduceAndConsumeKafkaService(properties.kafka) {
    override suspend fun sendAndReceive(request: DemoRequest, timeout: Duration?): DemoResponse {
        log.info { "Sending demo request $request" }

        val producerRecord = ProducerRecord<String, DemoRequest>(outputTopic, request)
        val consumerRecord = when (timeout) {
            null -> template.sendAndReceive(producerRecord)
            else -> template.sendAndReceive(producerRecord, timeout)
        }
        return consumerRecord.value()
    }
}
