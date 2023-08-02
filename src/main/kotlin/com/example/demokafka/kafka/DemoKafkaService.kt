package com.example.demokafka.kafka

import com.example.demokafka.kafka.dto.DemoRequest
import com.example.demokafka.kafka.dto.DemoResponse
import com.example.demokafka.properties.DemoKafkaProperties
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
import org.springframework.kafka.listener.ListenerUtils
import org.springframework.kafka.support.serializer.SerializationUtils
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.kafka.sender.SenderResult

private val log = KotlinLogging.logger { }

const val RQID = "RQID"

class DemoKafkaService(
    private val properties: DemoKafkaProperties,
    private val consumer: ReactiveKafkaConsumerTemplate<String, DemoRequest>,
    private val producer: ReactiveKafkaProducerTemplate<String, DemoResponse>,
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
            .subscribe()
    }

    private suspend fun processRecord(record: ConsumerRecord<String, DemoRequest>): SenderResult<Void>? {
        log.info {
            "\nRecord received\n" +
                    "\tkey : ${record.key()}\n" +
                    "\tvalue : ${record.value()}\n" +
                    "\theaders : ${record.headers()}"
        }
        val value = record.value()
        if (value == null) {
            val errorHeader = record.headers().lastHeader(SerializationUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER)
            if (errorHeader == null) {
                log.error { "Message value is empty!!!" }
            } else {
                log.error(
                    ListenerUtils.byteArrayToDeserializationException(
                        LogAccessor(DemoKafkaService::class.java),
                        errorHeader.value()
                    )
                ) {}
            }
            return null
        }
        val requestId = record.headers().lastHeader(RQID)
        if (requestId == null) {
            log.error { "RQID is empty!!!" }
            return null
        }
        val msg = value.msg
        return producer.send(
            ProducerRecord(
                properties.kafka.outputTopic, null, null, null,
                DemoResponse(msg.repeat(3)),
                RecordHeaders(listOf(RecordHeader(RQID, requestId.value())))
            )
        ).awaitSingle()
    }

    override fun destroy() {
        subscription?.dispose()
    }
}
