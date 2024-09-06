package com.example.demokafka.kafka.actuator

import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.beans.factory.DisposableBean
import org.springframework.boot.actuate.health.Health
import org.springframework.boot.actuate.health.ReactiveHealthIndicator
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate
import reactor.core.publisher.Mono
import reactor.kafka.sender.SenderOptions

private val log = KotlinLogging.logger {}

class KafkaHealthIndicator(
    senderOptions: SenderOptions<Long, Long>,
    private val topic: String,
) : ReactiveHealthIndicator, DisposableBean {
    private val producer: ReactiveKafkaProducerTemplate<Long, Long> = ReactiveKafkaProducerTemplate(senderOptions)

    override fun health(): Mono<Health> {
        val millis = System.currentTimeMillis()
        return producer.send(topic, millis, millis).map { senderResult ->
            val exception = senderResult.exception()
            if (exception != null) {
                Health.down().withTopic().withError(exception.message).build()
            } else {
                Health.up().withTopic().build()
            }
        }.onErrorResume { throwable ->
            Mono.just(Health.down().withTopic().withError(throwable.message).build())
        }
    }

    override fun destroy() = producer.destroy()

    private fun Health.Builder.withTopic() = this.withDetail("topic", topic)
    private fun Health.Builder.withError(msg: String?) = this.withDetail("error", msg)
}
