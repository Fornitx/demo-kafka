package com.example.demokafka.kafka

import com.example.demokafka.kafka.dto.DemoRequest
import com.example.demokafka.kafka.dto.DemoResponse
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.beans.factory.DisposableBean
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.core.log.LogAccessor
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate
import org.springframework.kafka.listener.ListenerUtils
import org.springframework.kafka.support.serializer.SerializationUtils
import org.springframework.messaging.support.GenericMessage
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.util.retry.Retry
import java.time.Duration

private val log = KotlinLogging.logger { }

class DemoKafkaService(
    private val properties: DemoKafkaProperties,
    private val consumer: ReactiveKafkaConsumerTemplate<String, DemoRequest>,
    private val producer: ReactiveKafkaProducerTemplate<String, DemoResponse>
) : DisposableBean {
    private var subscription: Disposable? = null

    @EventListener(ApplicationReadyEvent::class)
    fun ready() {
        subscription = Flux.defer {
            consumer.receiveAutoAck()
                .doOnSubscribe {
                    log.info { "Kafka Consumer started for topic ${properties.kafka.inputTopic}" }
                }
                .concatMap(::processRecord)
        }
//            .doOnError { ex -> log.error(ex) { } }
            .retryWhen(Retry.fixedDelay(Long.MAX_VALUE, Duration.ofSeconds(1)))
            .subscribe()
//        Thread.sleep(30_000)
    }

    private fun processRecord(record: ConsumerRecord<String, DemoRequest>): Mono<Void> {
        log.info {
            "\nRecord received\n" +
                "\tkey : ${record.key()}\n" +
                "\tvalue : ${record.value()}\n" +
                "\theaders : ${record.headers()}"
        }
        val value = record.value()
        if (value == null) {
            val errorHeader = record.headers().lastHeader(SerializationUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER)
            if (errorHeader != null) {
                log.error(
                    ListenerUtils.byteArrayToDeserializationException(
                        LogAccessor(DemoKafkaService::class.java),
                        errorHeader.value()
                    )
                ) { }
            }
            return Mono.empty()
        }
        val msg = value.msg
        val messages = listOf(
            GenericMessage(DemoResponse(msg.repeat(3)), mapOf("RQID" to 1)),
            GenericMessage(DemoResponse(msg.lowercase()), mapOf("RQID" to 2)),
        )
        return Flux.fromIterable(messages)
            .concatMap { producer.send(properties.kafka.outputTopic, it) }
            .then()
    }

    override fun destroy() {
        subscription?.dispose()
    }
}
