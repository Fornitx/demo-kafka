package com.example.demokafka.kafka.metrics

import com.example.demokafka.properties.PREFIX
import com.fasterxml.jackson.databind.util.NamingStrategyImpls
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.jetbrains.annotations.TestOnly
import org.jetbrains.annotations.VisibleForTesting
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap

const val METER_TAG_TOPIC = "topic"

@Component
class DemoKafkaMetrics(private val registry: MeterRegistry) {
    fun timer(): Timer.Sample = Timer.start(registry)

    fun kafkaConsume(topic: String): Counter = counter(
        "", DemoKafkaMetrics::kafkaConsume.name,
        METER_TAG_TOPIC, topic,
    )

    fun kafkaProduce(topic: String): Counter = counter(
        "", DemoKafkaMetrics::kafkaProduce.name,
        METER_TAG_TOPIC, topic,
    )

    fun kafkaProduceErrors(topic: String): Counter = counter(
        "", DemoKafkaMetrics::kafkaProduceErrors.name,
        METER_TAG_TOPIC, topic,
    )

    fun kafkaTiming(topic: String): Timer = timer(
        "", DemoKafkaMetrics::kafkaTiming.name,
        METER_TAG_TOPIC, topic,
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
        private val nameTranslator = NamingStrategyImpls.SNAKE_CASE

        @VisibleForTesting
        @TestOnly
        fun name(name: String): String = nameCache.computeIfAbsent(name) { key ->
            val name = nameTranslator.translate(key).replace("_", ".")
            "$PREFIX.$name"
        }
    }
}
