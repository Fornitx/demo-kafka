package com.example.demokafka

import com.example.demokafka.kafka.metrics.DemoKafkaMetrics
import io.micrometer.core.instrument.*
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.springframework.beans.factory.annotation.Autowired
import kotlin.reflect.KFunction
import kotlin.test.assertEquals
import kotlin.test.fail

abstract class AbstractMetricsTest : AbstractTest() {
    @Autowired
    private lateinit var registry: MeterRegistry

    @Autowired
    protected lateinit var metrics: DemoKafkaMetrics

    @AfterEach
    fun clearMetrics() {
        registry.clear()
    }

    protected fun assertNoMeter(name: String) {
        assertNoRawMeter(DemoKafkaMetrics.name(name))
    }

    protected fun assertNoRawMeter(name: String) {
        val meters = registry.find(name).meters()
        assertThat(meters)
            .`as` { "Check meters [${meters.map { it.id }}] is empty for meter '$name'" }
            .isEmpty()
    }

    protected fun assertMeter(func: KFunction<*>, tags: Map<String, String>, count: Int = 1) {
        assertRawMeter(DemoKafkaMetrics.name(func.name), tags, count)
    }

    protected fun assertRawMeter(name: String, tags: Map<String, String>, count: Int = 1) {
        assertRawMeters(name, mapOf(tags to count))
    }

    protected fun assertMeters(name: String, tagsCountMap: Map<Map<String, String>, Int>) {
        assertRawMeters(DemoKafkaMetrics.name(name), tagsCountMap)
    }

    protected fun assertRawMeters(name: String, tagsCountMap: Map<Map<String, String>, Int>) {
        val meters = registry.find(name).meters()
        assertThat(meters)
            .`as` { "Check meters [${meters.map { it.id }}] has size ${tagsCountMap.size} for meter '$name'" }
            .hasSize(tagsCountMap.size)

        for ((tags, count) in tagsCountMap) {
            val matchedMeter = meters.singleOrNull { it.matches(tags) }

            if (matchedMeter == null) {
                fail("Meter $name is not found by tags [$tags]")
            } else if (matchedMeter is Counter) {
                assertEquals(count.toDouble(), matchedMeter.count())
            } else if (matchedMeter is Timer) {
                assertEquals(count.toLong(), matchedMeter.count())
            } else if (matchedMeter is Gauge) {
                assertEquals(count.toDouble(), matchedMeter.value())
            } else {
                fail("Unsupported meter type '${matchedMeter::class}'")
            }
        }
    }

    private fun Meter.matches(tags: Map<String, String>): Boolean =
        tags.all { tag -> this.hasTag(tag) }

    private fun Meter.hasTag(entry: Map.Entry<String, String>): Boolean =
        this.id.tags.any { it.matches(entry) }

    private fun Tag.matches(entry: Map.Entry<String, String>): Boolean =
        this.key == entry.key && this.value == entry.value
}
