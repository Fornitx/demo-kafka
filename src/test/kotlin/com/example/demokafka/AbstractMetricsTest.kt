package com.example.demokafka

import com.example.demokafka.kafka.metrics.DemoKafkaMetrics
import io.micrometer.core.instrument.MeterRegistry
import org.junit.jupiter.api.AfterEach
import org.springframework.beans.factory.annotation.Autowired

abstract class AbstractMetricsTest : AbstractTest() {
    @Autowired
    private lateinit var registry: MeterRegistry

    @Autowired
    protected lateinit var metrics: DemoKafkaMetrics

    @AfterEach
    fun clearMetrics() {
        registry.clear()
    }
}
