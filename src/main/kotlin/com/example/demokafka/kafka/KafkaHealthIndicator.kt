package com.example.demokafka.kafka

import mu.KotlinLogging
import org.apache.kafka.common.utils.ThreadUtils
import org.springframework.beans.factory.DisposableBean
import org.springframework.boot.actuate.health.Health
import org.springframework.boot.actuate.health.HealthIndicator
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

private val log = KotlinLogging.logger {}

class KafkaHealthIndicator(private val kafka: DemoKafkaProperties.MyKafkaProperties) : HealthIndicator, DisposableBean {
    private val consumerFactory: DefaultKafkaConsumerFactory<ByteArray, ByteArray>
    private val executor: ScheduledExecutorService
    private val healthRef = AtomicReference(Health.unknown().withDetail("topic", kafka.inputTopic).build())

    init {
        val consumerProperties = kafka.buildConsumerProperties()
        consumerFactory = DefaultKafkaConsumerFactory<ByteArray, ByteArray>(consumerProperties)

        executor = Executors.newSingleThreadScheduledExecutor(ThreadUtils.createThreadFactory("KHI-%d", true))
        executor.scheduleWithFixedDelay({
            log.info { "start" }
            TimeUnit.SECONDS.sleep(6)
            try {
                consumerFactory.createConsumer().partitionsFor(kafka.inputTopic)
                healthRef.set(Health.up().withDetail("topic", kafka.inputTopic).build())
            } catch (ex: Exception) {
                log.error(ex) {}
                healthRef.set(Health.down().withDetail("topic", kafka.inputTopic).build())
            }
            log.info { "end" }
        }, 0L, kafka.healthCheckTimeout!!.toMillis(), TimeUnit.MILLISECONDS)
    }

    override fun health(): Health {
        return healthRef.get()
    }

    override fun destroy() {
        executor.shutdown()
    }
}
