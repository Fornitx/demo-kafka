package com.example.demokafka.kafka.services

import com.example.demokafka.kafka.metrics.DemoKafkaMetrics
import com.example.demokafka.kafka.model.DemoRequest
import com.example.demokafka.kafka.model.DemoResponse
import com.example.demokafka.properties.CustomKafkaProperties
import com.example.demokafka.properties.PREFIX
import com.example.demokafka.utils.*
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.future.await
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.record.TimestampType.*
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.kafka.support.KafkaUtils
import java.time.Duration
import java.util.concurrent.TimeUnit
import kotlin.math.abs

private val log = KotlinLogging.logger { }

class ConsumeAndProduceKafkaService(
    private val properties: CustomKafkaProperties,
    private val kafkaTemplate: KafkaTemplate<String, DemoResponse>,
    private val metrics: DemoKafkaMetrics,
) {
    private val outputTopic = properties.outputTopic

    @KafkaListener(topics = [$$"${$$PREFIX.kafka.consume-produce.input-topic}"])
    suspend fun consume(consumerRecord: ConsumerRecord<String, DemoRequest>) {
        val currentTimeMillis = System.currentTimeMillis()

        log.debug { consumerRecord.log() }
        metrics.kafkaConsume(consumerRecord.topic()).increment()

        when (consumerRecord.timestampType()) {
            null, NO_TIMESTAMP_TYPE -> {
                // do nothing
            }

            CREATE_TIME, LOG_APPEND_TIME -> {
                val consumerLag = Duration.ofMillis(abs(currentTimeMillis - consumerRecord.timestamp()))
                metrics.kafkaConsumeLag(consumerRecord.topic()).record(consumerLag)
                if (consumerLag >= properties.consumerLag) {
                    log.warn { "Obsolete record ${KafkaUtils.format(consumerRecord)}" }
                    return
                }
            }
        }

        try {
            processRecord(consumerRecord)
        } catch (ex: Exception) {
            log.error(ex) { "Unexpected error in Kafka consumer!!!" }
        } finally {
            metrics.kafkaTiming(consumerRecord.topic())
                .record(System.currentTimeMillis() - currentTimeMillis, TimeUnit.MILLISECONDS)
        }
    }

    private suspend fun processRecord(record: ConsumerRecord<String, DemoRequest>) {
        val exception = DemoKafkaUtils.checkForErrors(record)
        if (exception != null) {
            log.error(exception) {}
            return
        }

        val value = record.value()
        if (value == null) {
            log.error { "Message value is empty!!!" }
            return
        }

        val requestId = record.headers().lastHeader(Constants.RQID)
        if (requestId == null) {
            log.error { "RQID is empty!!!" }
            return
        }

        val replyTopic = record.headers().lastHeader(KafkaHeaders.REPLY_TOPIC)?.value()?.decodeToString()
            ?: outputTopic

        val replyPartition = record.headers().lastHeader(KafkaHeaders.REPLY_PARTITION)?.value()?.decodeToInt()

        val msg = value.msg1
        // TODO try/catch send
        val sendResult = kafkaTemplate.send(
            ProducerRecord<String, DemoResponse>(
                replyTopic, replyPartition, null,
                DemoResponse(msg.repeat(3)),
                headers(Constants.RQID to requestId.value())
            )
        ).await()

        if (sendResult == null) {
            metrics.kafkaProduceErrors(replyTopic).increment()
            log.error { "SendResult is null" }
        } else {
            metrics.kafkaProduce(replyTopic).increment()
            log.debug { "SendResult: $sendResult" }
        }
    }
}
