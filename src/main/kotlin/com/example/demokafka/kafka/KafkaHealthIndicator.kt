package com.example.demokafka.kafka

import com.example.demokafka.properties.DemoKafkaProperties.MyKafkaProperties
import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.common.utils.ThreadUtils
import org.springframework.beans.factory.DisposableBean
import org.springframework.boot.actuate.health.Health
import org.springframework.boot.actuate.health.HealthIndicator
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

private val log = KotlinLogging.logger {}

class KafkaHealthIndicator(
    private val springKafkaProperties: KafkaProperties,
    private val kafka: MyKafkaProperties
) : HealthIndicator, DisposableBean {
    private val consumerFactory: DefaultKafkaConsumerFactory<ByteArray, ByteArray>
    private val executor: ScheduledExecutorService

    private val healthRef = AtomicReference(Health.unknown().withTopic().build())
    private val healthUp = Health.up().withTopic().build()
    private val healthDown = Health.down().withTopic().build()

    private fun Health.Builder.withTopic() = this.withDetail("topic", kafka.inputTopic)

    init {
        val consumerProperties = springKafkaProperties.buildConsumerProperties() + kafka.buildConsumerProperties()
        consumerFactory = DefaultKafkaConsumerFactory<ByteArray, ByteArray>(consumerProperties)

        executor = Executors.newSingleThreadScheduledExecutor(ThreadUtils.createThreadFactory("KHI-%d", true))
        executor.scheduleWithFixedDelay({
            try {
                consumerFactory.createConsumer().use { it.partitionsFor(kafka.inputTopic, kafka.healthCheckTimeout) }
                healthRef.set(healthUp)
            } catch (ex: Exception) {
                log.error(ex) {}
                healthRef.set(healthDown)
            }
        }, 0L, kafka.healthCheckInterval!!.toMillis(), TimeUnit.MILLISECONDS)
    }

    override fun health(): Health {
        return healthRef.get()
    }

    override fun destroy() {
        executor.shutdown()
    }
}
