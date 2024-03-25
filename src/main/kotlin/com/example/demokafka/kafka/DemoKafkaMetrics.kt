package com.example.demokafka.kafka

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.springframework.stereotype.Component

const val METRICS_TAG_TOPIC = "topic"

@Component
class DemoKafkaMetrics(private val registry: MeterRegistry) {
    fun kafkaConsume(topic: String): Counter = counter(
        "", DemoKafkaMetrics::kafkaConsume.name,
        METRICS_TAG_TOPIC, topic,
    )

    fun kafkaProduce(topic: String): Counter = counter(
        "", DemoKafkaMetrics::kafkaProduce.name,
        METRICS_TAG_TOPIC, topic,
    )

    fun kafkaTiming(topic: String): Timer = timer(
        "", DemoKafkaMetrics::kafkaTiming.name,
        METRICS_TAG_TOPIC, topic,
    )

    private fun counter(description: String, name: String, vararg tags: String): Counter {
        return Counter.builder(name(name)).description(description).tags(*tags).register(registry)
    }

    private fun timer(description: String, name: String, vararg tags: String): Timer {
        return Timer.builder(name(name)).description(description).tags(*tags).register(registry)
    }

    private fun gauge(
        description: String,
        name: String,
        vararg tags: String,
        numberSupplier: () -> Number
    ): Gauge {
        return Gauge.builder(name(name), numberSupplier).description(description).tags(*tags).register(registry)
    }

    companion object {
        fun name(name: String): String = "demo_$name"
    }
}
