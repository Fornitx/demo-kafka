package com.example.demokafka.kafka

import com.example.demokafka.kafka.dto.DemoRequest
import com.fasterxml.jackson.databind.ObjectMapper
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.beans.factory.DisposableBean
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.util.retry.Retry
import java.time.Duration

private val log = KotlinLogging.logger { }

class DemoKafkaService(
    private val properties: DemoKafkaProperties,
    private val objectMapper: ObjectMapper,
    private val consumer: ReactiveKafkaConsumerTemplate<String, DemoRequest>,
    private val producer: ReactiveKafkaProducerTemplate<String, String>
) : DisposableBean {
    private var subscription: Disposable? = null

    @EventListener(ApplicationReadyEvent::class)
    fun ready() {
        val block = producer.send(properties.kafka.inputTopic, """
            {"msg": "123"}
        """.trimIndent()).block()
        log.info { "Sent $block" }

        subscription = Flux.defer {
            consumer.receiveAutoAck()
                .doOnSubscribe {
                    log.info { "Kafka Consumer started for topic ${properties.kafka.inputTopic}" }
                }
                .concatMap(::processRecord)
        }
            .doOnError { ex -> log.error(ex) { } }
            .retryWhen(Retry.fixedDelay(Long.MAX_VALUE, Duration.ofSeconds(1)))
            .subscribe()
    }

    private fun processRecord(record: ConsumerRecord<String, DemoRequest>): Mono<Void> {
        log.info { Thread.currentThread() }
        log.info { Thread.currentThread().threadGroup }
        log.info {
            "\nRecord received\n" +
                "\tkey : ${record.key()}\n" +
                "\tvalue : ${record.value()}\n" +
                "\theaders : ${record.headers()}"
        }
        return Mono.empty()
//        val value = record.value()
//        if (value == null) {
//            val errorHeader = record.headers().lastHeader(SerializationUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER)
//            if (errorHeader != null) {
//                log.error(
//                    ListenerUtils.byteArrayToDeserializationException(
//                        LogAccessor(DemoKafkaService::class.java),
//                        errorHeader.value()
//                    )
//                ) { }
//            }
//            return Mono.empty()
//        }
//        val msg = value.msg
//        val messages = listOf(
//            GenericMessage(DemoResponse(msg.repeat(3)), mapOf("RQID" to 1)),
//            GenericMessage(DemoResponse(msg.lowercase()), mapOf("RQID" to 2)),
//        )
//        return Flux.fromIterable(messages)
//            .concatMap { producer.send(properties.kafka.outputTopic, it) }
//            .then()
    }

    override fun destroy() {
        subscription?.dispose()
    }
}
