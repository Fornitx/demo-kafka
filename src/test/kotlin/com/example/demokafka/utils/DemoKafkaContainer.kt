package com.example.demokafka.utils

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.common.config.TopicConfig
import org.springframework.kafka.config.TopicBuilder
import org.testcontainers.kafka.KafkaContainer

private val log = KotlinLogging.logger {}


private const val healthTopic = "health_check"
private val topics = listOf(
    "consume_produce_input",
    "consume_produce_output",
    "produce_consume_input",
    "produce_consume_output",
)

object DemoKafkaContainer : KafkaContainer(TestcontainersHelper.KAFKA_IMAGE) {
    init {
        withReuse(true)
//        withCreateContainerCmdModifier { createContainerCmd ->
//            createContainerCmd.hostConfig!!.withPortBindings(
//                PortBinding(
//                    Ports.Binding.bindPort(0),
//                    ExposedPort(9092),
//                )
//            )
//        }
    }

    override fun start() {
        super.start()

        val bootstrapServers = bootstrapServers
        log.info { "KafkaContainer started: $bootstrapServers" }

        System.setProperty("TC_KAFKA", bootstrapServers)

        createTopics()
    }

    private fun createTopics() {
        AdminClient.create(
            mapOf(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers)
        ).use { adminClient ->
            val topics = topics.map { TopicBuilder.name(it).partitions(1).build() }
                .plus(
                    TopicBuilder.name(healthTopic)
                        .partitions(1)
                        .replicas(1)
                        .config(TopicConfig.RETENTION_BYTES_CONFIG, (1024L * 1024L).toString())
                        .build()
                )

            adminClient.createTopics(topics)//.all().get()
        }
    }
}
