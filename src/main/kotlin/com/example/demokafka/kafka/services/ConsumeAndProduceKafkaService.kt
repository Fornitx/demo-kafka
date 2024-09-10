package com.example.demokafka.kafka.services

import com.example.demokafka.kafka.metrics.DemoKafkaMetrics
import com.example.demokafka.kafka.model.DemoRequest
import com.example.demokafka.kafka.model.DemoResponse
import com.example.demokafka.properties.DemoKafkaProperties
import com.example.demokafka.utils.Constants
import com.example.demokafka.utils.DemoKafkaUtils
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.mono
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.TimestampType.*
import org.springframework.beans.factory.DisposableBean
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.kafka.support.KafkaUtils
import reactor.core.Disposable
import reactor.core.publisher.Flux
import kotlin.math.abs

private val log = KotlinLogging.logger { }

class ConsumeAndProduceKafkaService(
    private val properties: DemoKafkaProperties,
    private val consumer: ReactiveKafkaConsumerTemplate<String, DemoRequest>,
    private val producer: ReactiveKafkaProducerTemplate<String, DemoResponse>,
    private val metrics: DemoKafkaMetrics,
) : DisposableBean {
    private var subscription: Disposable? = null

    @EventListener(ApplicationReadyEvent::class)
    fun ready() {
        subscription = Flux.defer {
            consumer.receiveAutoAck()
                .timestamp()
                .doOnSubscribe {
                    log.info { "Kafka Consumer started for topic ${properties.inOutKafka.inputTopic}" }
                }
                .filter { filterObsolete(it.t2) }
                .concatMap {
                    mono {
                        processRecord(it.t2)
                    }
                }
        }.doOnError { throwable ->
            log.error(throwable) { "Unexpected error in Kafka consumer!!!" }
        }
            .retry()
            .subscribe()
    }

    private fun filterObsolete(record: ConsumerRecord<*, *>): Boolean = when (record.timestampType()) {
        null, NO_TIMESTAMP_TYPE -> true
        CREATE_TIME, LOG_APPEND_TIME -> {
            if (abs(System.currentTimeMillis() - record.timestamp()) < 5000) {
                true
            } else {
                log.warn { "Obsolete record ${KafkaUtils.format(record)}" }
                false
            }
        }
    }

    private suspend fun processRecord(record: ConsumerRecord<String, DemoRequest>) {
        log.debug {
            "\nRecord received\n" +
                    "\tkey : ${record.key()}\n" +
                    "\tvalue : ${record.value()}\n" +
                    "\theaders : ${record.headers()}"
        }
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
            ?: properties.inOutKafka.outputTopic
        val msg = value.msg
        val senderResult = producer.send(
            ProducerRecord(
                replyTopic, null, null, null,
                DemoResponse(msg.repeat(3)),
                RecordHeaders(listOf(RecordHeader(Constants.RQID, requestId.value())))
            )
        ).awaitSingle()

        if (senderResult.exception() == null) {
            metrics.kafkaProduce(replyTopic).increment()
            log.debug { "SenderResult: $senderResult" }
        } else {
            log.error(senderResult.exception()) { "SenderResult: $senderResult" }
            metrics.kafkaProduceErrors(replyTopic).increment()
        }
    }

    override fun destroy() {
        subscription?.dispose()
    }
}
