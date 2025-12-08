package com.example.demokafka.kafka.metrics

import com.example.demokafka.properties.PREFIX
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.jetbrains.annotations.TestOnly
import org.jetbrains.annotations.VisibleForTesting
import org.springframework.data.util.ParsingUtils
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KFunction

const val METER_TAG_TOPIC = "topic"

@Component
class DemoKafkaMetrics(private val registry: MeterRegistry) {
    fun timer(): Timer.Sample = Timer.start(registry)

    fun kafkaConsumeLag(topic: String): Timer = timer(
        "", DemoKafkaMetrics::kafkaConsumeLag,
        METER_TAG_TOPIC, topic,
    )

    fun kafkaConsume(topic: String): Counter = counter(
        "", DemoKafkaMetrics::kafkaConsume,
        METER_TAG_TOPIC, topic,
    )

    fun kafkaProduce(topic: String): Counter = counter(
        "", DemoKafkaMetrics::kafkaProduce,
        METER_TAG_TOPIC, topic,
    )

    fun kafkaProduceErrors(topic: String): Counter = counter(
        "", DemoKafkaMetrics::kafkaProduceErrors,
        METER_TAG_TOPIC, topic,
    )

    fun kafkaTiming(topic: String): Timer = timer(
        "", DemoKafkaMetrics::kafkaTiming,
        METER_TAG_TOPIC, topic,
    )

    private fun counter(description: String, func: KFunction<*>, vararg tags: String): Counter =
        Counter.builder(name(func)).description(description).tags(*tags).register(registry)

    private fun timer(description: String, func: KFunction<*>, vararg tags: String): Timer =
        Timer.builder(name(func)).description(description).tags(*tags).register(registry)

    private fun gauge(
        description: String,
        func: KFunction<*>,
        vararg tags: String,
        numberSupplier: () -> Number
    ): Gauge = Gauge.builder(name(func), numberSupplier).description(description).tags(*tags).register(registry)

    companion object {
        private val nameCache = ConcurrentHashMap<String, String>()

        @VisibleForTesting
        @TestOnly
        fun name(func: KFunction<*>): String = nameCache.computeIfAbsent(func.name) { key ->
            val name = ParsingUtils.reconcatenateCamelCase(key, ".")
            "$PREFIX.$name"
        }
    }
}
