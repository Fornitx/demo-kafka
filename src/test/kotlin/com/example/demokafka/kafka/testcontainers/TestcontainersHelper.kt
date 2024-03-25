package com.example.demokafka.kafka.testcontainers

import io.github.oshai.kotlinlogging.KotlinLogging
import org.testcontainers.containers.KafkaContainer

private val log = KotlinLogging.logger { }

object TestcontainersHelper {
    @JvmStatic
    val kafkaContainer: KafkaContainer = KafkaContainer()
        .withReuse(true)

    init {
        kafkaContainer.start()
        log.info { "KafkaContainer started on port ${kafkaContainer.bootstrapServers}" }
        System.setProperty("TC_KAFKA", kafkaContainer.bootstrapServers)
    }
}
