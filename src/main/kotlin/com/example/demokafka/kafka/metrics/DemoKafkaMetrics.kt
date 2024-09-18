package com.example.demokafka.kafka.metrics

import com.example.demokafka.properties.PREFIX
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap

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

    fun kafkaProduceErrors(topic: String): Counter = counter(
        "", DemoKafkaMetrics::kafkaProduceErrors.name,
        METRICS_TAG_TOPIC, topic,
    )

    fun kafkaTiming(topic: String): Timer = timer(
        "", DemoKafkaMetrics::kafkaTiming.name,
        METRICS_TAG_TOPIC, topic,
    )

    private fun counter(description: String, name: String, vararg tags: String): Counter =
        Counter.builder(name(name)).description(description).tags(*tags).register(registry)

    private fun timer(description: String, name: String, vararg tags: String): Timer =
        Timer.builder(name(name)).description(description).tags(*tags).register(registry)

    private fun gauge(
        description: String,
        name: String,
        vararg tags: String,
        numberSupplier: () -> Number
    ): Gauge = Gauge.builder(name(name), numberSupplier).description(description).tags(*tags).register(registry)

    companion object {
        private val nameCache = ConcurrentHashMap<String, String>()

        private fun name(name: String): String = nameCache.computeIfAbsent(name) {
            PREFIX + "_" + name
        }
    }
}
