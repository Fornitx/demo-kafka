package com.example.demokafka.kafka.actuator

import org.springframework.beans.factory.DisposableBean
import org.springframework.boot.actuate.health.Health
import org.springframework.boot.actuate.health.ReactiveHealthIndicator
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate
import reactor.core.publisher.Mono
import reactor.kafka.sender.SenderOptions

class KafkaHealthIndicator(
    senderOptions: SenderOptions<Long, Long>,
    private val topic: String,
) : ReactiveHealthIndicator, DisposableBean {
    private val producer: ReactiveKafkaProducerTemplate<Long, Long> = ReactiveKafkaProducerTemplate(senderOptions)

    private val healthUp = Health.up().withTopic(topic).build()

    override fun health(): Mono<Health> {
        val millis = System.currentTimeMillis()
        return producer.send(topic, millis, millis).map { senderResult ->
            val exception = senderResult.exception()
            if (exception == null) {
                healthUp
            } else {
                Health.down().withTopic(topic).withError(exception.message).build()
            }
        }.onErrorResume { throwable ->
            Mono.just(Health.down().withTopic(topic).withError(throwable.message).build())
        }
    }

    override fun destroy() = producer.destroy()

    private fun Health.Builder.withTopic(topic: String): Health.Builder = this.withDetail("topic", topic)
    private fun Health.Builder.withError(msg: String?): Health.Builder = this.withDetail("error", msg)
}
