package com.example.demokafka.kafka

import com.example.demokafka.kafka.model.DemoRequest
import com.example.demokafka.kafka.model.DemoResponse
import com.example.demokafka.properties.DemoKafkaProperties
import com.example.demokafka.utils.Constants.RQID
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.mono
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.header.internals.RecordHeaders
import org.springframework.beans.factory.DisposableBean
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.core.log.LogAccessor
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.kafka.support.serializer.SerializationUtils
import reactor.core.Disposable
import reactor.core.publisher.Flux

private val log = KotlinLogging.logger { }

class DemoKafkaService(
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
                .doOnSubscribe {
                    log.info { "Kafka Consumer started for topic ${properties.kafka.inputTopic}" }
                }
                .concatMap {
                    mono {
                        try {
                            processRecord(it)
                        } catch (ex: Exception) {
                            log.error(ex) { "Unexpected error in Kafka consumer!!!" }
                        }
                    }
                }
        }
//            .retry()
            .subscribe()
    }

    private suspend fun processRecord(record: ConsumerRecord<String, DemoRequest>) {
        log.debug {
            "\nRecord received\n" +
                    "\tkey : ${record.key()}\n" +
                    "\tvalue : ${record.value()}\n" +
                    "\theaders : ${record.headers()}"
        }
        metrics.kafkaConsume(record.topic()).increment()

        val value = record.value()
        if (value == null) {
            val errorHeader = record.headers().lastHeader(SerializationUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER)
            if (errorHeader == null) {
                log.error { "Message value is empty!!!" }
            } else {
                log.error(
                    SerializationUtils.byteArrayToDeserializationException(
                        LogAccessor(DemoKafkaService::class.java),
                        errorHeader
                    )
                ) {}
            }
            return
        }
        val requestId = record.headers().lastHeader(RQID)
        if (requestId == null) {
            log.error { "RQID is empty!!!" }
            return
        }
        val replyTopic = record.headers().lastHeader(KafkaHeaders.REPLY_TOPIC)
        if (replyTopic == null) {
            log.error { "${KafkaHeaders.REPLY_TOPIC} is empty!!!" }
            return
        }
        val replyTopicValue = replyTopic.value().decodeToString()
        val msg = value.msg
        val senderResult = producer.send(
            ProducerRecord(
                replyTopicValue, null, null, null,
                DemoResponse(msg.repeat(3)),
                RecordHeaders(listOf(RecordHeader(RQID, requestId.value())))
            )
        ).awaitSingle()
        metrics.kafkaProduce(replyTopicValue).increment()
        log.debug { "SenderResult: $senderResult" }
    }

    override fun destroy() {
        subscription?.dispose()
    }
}
