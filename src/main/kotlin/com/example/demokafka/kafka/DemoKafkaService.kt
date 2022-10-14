package com.example.demokafka.kafka

import com.example.demokafka.kafka.dto.DemoRequest
import com.example.demokafka.kafka.dto.DemoResponse
import mu.KotlinLogging
import org.springframework.beans.factory.DisposableBean
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate
import org.springframework.messaging.support.GenericMessage
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import reactor.kafka.receiver.ReceiverRecord

private val log = KotlinLogging.logger { }

class DemoKafkaService(
    private val properties: DemoKafkaProperties,
    private val consumer: ReactiveKafkaConsumerTemplate<String, DemoRequest>,
    private val producer: ReactiveKafkaProducerTemplate<String, DemoResponse>
) : DisposableBean {
    private val scheduler = Schedulers.newBoundedElastic(10, Integer.MAX_VALUE, "demo-kafka")
    private var subscription: Disposable? = null

    @EventListener(ApplicationReadyEvent::class)
    fun ready() {
        subscription = consumer.receive()
            .doOnSubscribe {
                log.info { "Kafka Consumer started for topic ${properties.kafka.inputTopic}" }
            }
            .groupBy { it.receiverOffset().topicPartition() }
            .flatMap { partitionFlux ->
                partitionFlux.publishOn(scheduler)
                    .concatMap { record ->
                        processRecord(record).doAfterTerminate {
                            record.receiverOffset().acknowledge()
                        }
                    }
            }
            .subscribe()
    }

    private fun processRecord(record: ReceiverRecord<String, DemoRequest>): Mono<Void> {
        log.info {
            "\nRecord received\n" +
                "\tkey : ${record.key()}\n" +
                "\tvalue : ${record.value()}\n" +
                "\theaders : ${record.headers()}"
        }
        val msg = record.value().msg
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
        scheduler.dispose()
    }
}
