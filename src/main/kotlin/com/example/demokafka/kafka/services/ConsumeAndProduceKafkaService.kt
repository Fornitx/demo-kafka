package com.example.demokafka.kafka.services

import com.example.demokafka.kafka.metrics.DemoKafkaMetrics
import com.example.demokafka.kafka.model.DemoRequest
import com.example.demokafka.kafka.model.DemoResponse
import com.example.demokafka.properties.CustomKafkaProperties
import com.example.demokafka.utils.Constants
import com.example.demokafka.utils.DemoKafkaUtils
import com.example.demokafka.utils.log
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.reactor.awaitSingleOrNull
import kotlinx.coroutines.reactor.mono
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.header.internals.RecordHeaders
import org.springframework.beans.factory.DisposableBean
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate
import org.springframework.kafka.support.KafkaHeaders
import reactor.core.Disposable
import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit

private val log = KotlinLogging.logger { }

class ConsumeAndProduceKafkaService(
    private val properties: CustomKafkaProperties,
    private val consumer: ReactiveKafkaConsumerTemplate<String, DemoRequest>,
    private val producer: ReactiveKafkaProducerTemplate<String, DemoResponse>,
    private val metrics: DemoKafkaMetrics,
) : DisposableBean {
    private val inputTopic = properties.inputTopic
    private val outputTopic = properties.outputTopic

    private var subscription: Disposable? = null

    @EventListener(ApplicationReadyEvent::class)
    fun startConsumer() {
        subscription = consumer.receiveAutoAck()
            .timestamp()
            .doOnSubscribe {
                log.info { "Kafka Consumer started for topic $inputTopic" }
            }
//            .filter { filterObsolete(it.t2) }
            .concatMap {
                mono {
                    try {
                        processRecord(it.t2)
                    } catch (ex: Exception) {
                        log.error(ex) { "Unexpected error in Kafka consumer!!!" }
                    } finally {
                        metrics.kafkaTiming(inputTopic)
                            .record(System.currentTimeMillis() - it.t1, TimeUnit.MILLISECONDS)
                    }
                }
            }
            .subscribe()
    }

//    private fun filterObsolete(record: ConsumerRecord<*, *>): Boolean = when (record.timestampType()) {
//        null, NO_TIMESTAMP_TYPE -> true
//        CREATE_TIME, LOG_APPEND_TIME -> {
//            if (abs(System.currentTimeMillis() - record.timestamp()) < 5000) {
//                true
//            } else {
//                log.warn { "Obsolete record ${KafkaUtils.format(record)}" }
//                false
//            }
//        }
//    }

    private suspend fun processRecord(record: ConsumerRecord<String, DemoRequest>) {
        log.debug { record.log() }
        metrics.kafkaConsume(record.topic()).increment()

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

        // TODO test this
        val replyPartition = record.headers().lastHeader(KafkaHeaders.REPLY_PARTITION)?.value()?.decodeToInt()

        val msg = value.msg
        val senderResult = producer.send(
            ProducerRecord(
                replyTopic, replyPartition, null, null,
                DemoResponse(msg.repeat(3)),
                RecordHeaders(listOf(RecordHeader(Constants.RQID, requestId.value())))
            )
        ).awaitSingleOrNull()

        if (senderResult == null) {
            metrics.kafkaProduceErrors(replyTopic).increment()
            log.error { "SenderResult is null" }
        } else if (senderResult.exception() != null) {
            metrics.kafkaProduceErrors(replyTopic).increment()
            log.error(senderResult.exception()) { "SenderResult: $senderResult" }
        } else {
            metrics.kafkaProduce(replyTopic).increment()
            log.debug { "SenderResult: $senderResult" }
        }
    }

    override fun destroy() {
        subscription?.dispose()
    }

    private fun ByteArray.decodeToInt(): Int = ByteBuffer.wrap(this).int
}
