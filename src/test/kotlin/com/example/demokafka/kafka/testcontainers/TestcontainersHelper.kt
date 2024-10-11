package com.example.demokafka.kafka.testcontainers

import com.github.dockerjava.api.model.ExposedPort
import com.github.dockerjava.api.model.PortBinding
import com.github.dockerjava.api.model.Ports
import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.common.config.TopicConfig
import org.springframework.kafka.config.TopicBuilder
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName
import org.testcontainers.utility.TestcontainersConfiguration

private val log = KotlinLogging.logger {}

object TestcontainersHelper {
    private const val healthTopic: String = "health_topic_tc"
    private val topics: List<String> = listOf(
        "in_out_in_topic_tc",
        "in_out_out_topic_tc",
        "out_in_in_topic_tc",
        "out_in_out_topic_tc",
    )

    private val tcCfg: TestcontainersConfiguration = TestcontainersConfiguration.getInstance()

//    val KAFKA_CONTAINER_OLD: org.testcontainers.containers.KafkaContainer by lazy {
//        val kafkaContainerImage = tcCfg.getEnvVarOrProperty("kafka.container.image.old", "")
//        val dockerImageName = DockerImageName.parse(kafkaContainerImage)
//            .asCompatibleSubstituteFor("confluentinc/cp-kafka")
//        val kafkaContainer = org.testcontainers.containers.KafkaContainer(dockerImageName)
//            .withCreateContainerCmdModifier { createContainerCmd ->
//                createContainerCmd.hostConfig!!.withPortBindings(
//                    PortBinding(
//                        Ports.Binding.bindPort(org.testcontainers.containers.KafkaContainer.KAFKA_PORT),
//                        ExposedPort(org.testcontainers.containers.KafkaContainer.KAFKA_PORT)
//                    ),
//                    PortBinding(
//                        Ports.Binding.bindPort(org.testcontainers.containers.KafkaContainer.ZOOKEEPER_PORT),
//                        ExposedPort(org.testcontainers.containers.KafkaContainer.ZOOKEEPER_PORT)
//                    )
//                )
//            }
//            .withReuse(true)
//
//        kafkaContainer.start()
//
//        val bootstrapServers = kafkaContainer.bootstrapServers
//        log.info { "Old KafkaContainer started on port $bootstrapServers" }
//        System.setProperty("TC_KAFKA", bootstrapServers)
//
//        createTopics(bootstrapServers)
//
//        kafkaContainer
//    }

    val KAFKA_CONTAINER_NEW: KafkaContainer by lazy {
        val kafkaContainerImage = tcCfg.getEnvVarOrProperty("kafka.container.image.new", "")
        val dockerImageName = DockerImageName.parse(kafkaContainerImage)
            .asCompatibleSubstituteFor("apache/kafka")
        val kafkaContainer = KafkaContainer(dockerImageName)
            .withCreateContainerCmdModifier { createContainerCmd ->
                createContainerCmd.hostConfig!!.withPortBindings(
                    PortBinding(
                        Ports.Binding.bindPort(0),
                        ExposedPort(9092)
                    )
                )
            }
            .withReuse(true)

        kafkaContainer.start()

        val bootstrapServers = kafkaContainer.bootstrapServers
        log.info { "New KafkaContainer started on port $bootstrapServers" }
        System.setProperty("TC_KAFKA", bootstrapServers)

        createTopics(bootstrapServers)

        kafkaContainer
    }

    private fun createTopics(bootstrapServers: String) {
        AdminClient.create(
            mapOf(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers)
        ).use { adminClient ->
            val topics = topics.map { TopicBuilder.name(it).partitions(1).build() }
                .plus(
                    TopicBuilder.name(healthTopic)
                        .partitions(1)
                        .config(TopicConfig.RETENTION_BYTES_CONFIG, (1024L * 1024L).toString())
                        .build()
                )

            adminClient.createTopics(topics)//.all().get()
        }
    }
}
