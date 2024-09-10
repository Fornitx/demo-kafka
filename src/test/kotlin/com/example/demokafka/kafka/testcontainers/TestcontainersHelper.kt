package com.example.demokafka.kafka.testcontainers

import com.github.dockerjava.api.model.ExposedPort
import com.github.dockerjava.api.model.PortBinding
import com.github.dockerjava.api.model.Ports
import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.testcontainers.utility.DockerImageName
import org.testcontainers.utility.TestcontainersConfiguration

private val log = KotlinLogging.logger {}

object TestcontainersHelper {
    private val tcCfg = TestcontainersConfiguration.getInstance()

    val KAFKA_CONTAINER_OLD: org.testcontainers.containers.KafkaContainer by lazy {
        val kafkaContainerImage = tcCfg.getEnvVarOrProperty("kafka.container.image.old", "")
        val dockerImageName = DockerImageName.parse(kafkaContainerImage)
            .asCompatibleSubstituteFor("confluentinc/cp-kafka")
        val kafkaContainer = org.testcontainers.containers.KafkaContainer(dockerImageName)
            .withCreateContainerCmdModifier { createContainerCmd ->
                createContainerCmd.hostConfig!!.withPortBindings(
                    PortBinding(
                        Ports.Binding.bindPort(org.testcontainers.containers.KafkaContainer.KAFKA_PORT),
                        ExposedPort(org.testcontainers.containers.KafkaContainer.KAFKA_PORT)
                    ),
                    PortBinding(
                        Ports.Binding.bindPort(org.testcontainers.containers.KafkaContainer.ZOOKEEPER_PORT),
                        ExposedPort(org.testcontainers.containers.KafkaContainer.ZOOKEEPER_PORT)
                    )
                )
            }
            .withReuse(true)

        kafkaContainer.start()
        log.info { "Old KafkaContainer started on port ${kafkaContainer.bootstrapServers}" }
        System.setProperty("TC_KAFKA", kafkaContainer.bootstrapServers)

        AdminClient.create(
            mapOf(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaContainer.bootstrapServers)
        ).use { adminClient ->
            adminClient.createTopics(listOf(/* TODO topics */)).all().get()
        }

        kafkaContainer
    }

    val KAFKA_CONTAINER_NEW: org.testcontainers.kafka.KafkaContainer by lazy {
        val tcCfg = TestcontainersConfiguration.getInstance()
        val kafkaContainerImage = tcCfg.getEnvVarOrProperty("kafka.container.image.new", "")
        val dockerImageName = DockerImageName.parse(kafkaContainerImage)
            .asCompatibleSubstituteFor("apache/kafka")
        val kafkaContainer = org.testcontainers.kafka.KafkaContainer(dockerImageName)
            .withCreateContainerCmdModifier { createContainerCmd ->
                createContainerCmd.hostConfig!!.withPortBindings(
                    PortBinding(
                        Ports.Binding.bindPort(9092),
                        ExposedPort(9092)
                    )
                )
            }
            .withReuse(true)

        kafkaContainer.start()
        log.info { "New KafkaContainer started on port ${kafkaContainer.bootstrapServers}" }
        System.setProperty("TC_KAFKA", kafkaContainer.bootstrapServers)

        AdminClient.create(
            mapOf(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaContainer.bootstrapServers)
        ).use { adminClient ->
            adminClient.createTopics(listOf(/* TODO topics */)).all().get()
        }

        kafkaContainer
    }
}
