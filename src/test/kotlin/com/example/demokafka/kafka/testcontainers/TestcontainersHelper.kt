package com.example.demokafka.kafka.testcontainers

import com.github.dockerjava.api.model.ExposedPort
import com.github.dockerjava.api.model.PortBinding
import com.github.dockerjava.api.model.Ports
import io.github.oshai.kotlinlogging.KotlinLogging
import org.testcontainers.containers.KafkaContainer

private val log = KotlinLogging.logger { }

object TestcontainersHelper {
    @JvmStatic
    val kafkaContainer: KafkaContainer = KafkaContainer()
        .withCreateContainerCmdModifier { createContainerCmd ->
            createContainerCmd.hostConfig!!.withPortBindings(
                PortBinding(
                    Ports.Binding.bindPort(KafkaContainer.KAFKA_PORT),
                    ExposedPort(KafkaContainer.KAFKA_PORT)
                ),
                PortBinding(
                    Ports.Binding.bindPort(KafkaContainer.ZOOKEEPER_PORT),
                    ExposedPort(KafkaContainer.ZOOKEEPER_PORT)
                )
            )
        }
        .withReuse(true)

    init {
        kafkaContainer.start()
        log.info { "KafkaContainer started on port ${kafkaContainer.bootstrapServers}" }
        System.setProperty("TC_KAFKA", kafkaContainer.bootstrapServers)
    }
}
