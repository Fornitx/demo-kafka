package com.example.demokafka.utils

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.config.TopicConfig
import org.springframework.kafka.config.TopicBuilder
import org.testcontainers.kafka.KafkaContainer

private val log = KotlinLogging.logger {}

object DemoKafkaContainer : KafkaContainer(TestcontainersHelper.KAFKA_IMAGE) {
    init {
        withReuse(true)
    }
//    override fun getBootstrapServers(): String = "%s:%s".format(host, getMappedPort(9093))

//    override fun configure() {
//        super.configure()
////        withEnv("KAFKA_NODE_ID", "1")
////        withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "SSL:SSL")
////        withEnv("KAFKA_ADVERTISED_LISTENERS", "SSL://localhost:9093")
////        withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
////        withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
////        withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
////        withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
////        withEnv("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR", "1")
////        withEnv("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR", "1")
////        withEnv("KAFKA_PROCESS_ROLES", "broker,controller")
////        withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@broker:29093")
////        withEnv("KAFKA_LISTENERS", "SSL://:9093")
////        withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "SSL-INTERNAL")
////        withEnv("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
////        withEnv("KAFKA_LOG_DIRS", "/tmp/kraft-combined-logs")
////        withEnv("CLUSTER_ID", "4L6g3nShT-eMCtK--X86sw")
////        withEnv("KAFKA_SSL_KEYSTORE_FILENAME", "test-server.p12")
////        withEnv("KAFKA_SSL_KEYSTORE_CREDENTIALS", "credentials")
////        withEnv("KAFKA_SSL_KEY_CREDENTIALS", "credentials")
////        withEnv("KAFKA_SSL_TRUSTSTORE_FILENAME", "test-ca.p12")
////        withEnv("KAFKA_SSL_TRUSTSTORE_CREDENTIALS", "credentials")
////        withEnv("KAFKA_SSL_CLIENT_AUTH", "required")
////        withEnv("KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM", "")
//
//
////        withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "PLAINTEXT:SSL,BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT")
////        withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")
//        withEnv("KAFKA_SSL_CLIENT_AUTH", "required")
//        withEnv("KAFKA_SSL_KEYSTORE_LOCATION", "/etc/kafka/secrets/test-server.p12")
//        withEnv("KAFKA_SSL_KEYSTORE_PASSWORD", "password")
//        withEnv("KAFKA_SSL_KEYSTORE_TYPE", "PKCS12")
//        withEnv("KAFKA_SSL_KEY_PASSWORD", "password")
//        withEnv("KAFKA_SSL_TRUSTSTORE_LOCATION", "/etc/kafka/secrets/test-ca.p12")
//        withEnv("KAFKA_SSL_TRUSTSTORE_PASSWORD", "password")
//        withEnv("KAFKA_SSL_TRUSTSTORE_TYPE", "PKCS12")
//        withEnv("KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM", "")
////        withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "BROKER:PLAINTEXT,SSL:SSL,CONTROLLER:PLAINTEXT")
////        withEnv("KAFKA_LISTENERS", "BROKER://0.0.0.0:9093,SSL://0.0.0.0:9092,CONTROLLER://0.0.0.0:9094")
////        withEnv("KAFKA_ADVERTISED_LISTENERS", "SSL://localhost:9092,BROKER://localhost:9093")
//
//        withCopyFileToContainer(
//            MountableFile.forHostPath("etc/ssl/test-server.p12"),
//            "/etc/kafka/secrets/test-server.p12"
//        )
////        withCopyFileToContainer(
////            MountableFile.forHostPath("etc/ssl/credentials"),
////            "/etc/kafka/secrets/credentials"
////        )
//        withCopyFileToContainer(
//            MountableFile.forHostPath("etc/ssl/test-ca.p12"),
//            "/etc/kafka/secrets/test-ca.p12"
//        )
//
//        withCreateContainerCmdModifier { createContainerCmd ->
//            createContainerCmd.hostConfig!!.withPortBindings(
//                PortBinding(
//                    Ports.Binding.bindPort(0),
//                    ExposedPort(9093),
//                )
//            )
//        }
//
//        withReuse(true)
//    }

    override fun start() {
        super.start()

        log.info { "KafkaContainer started: $bootstrapServers" }

        System.setProperty("TC_KAFKA", bootstrapServers)

        createTopics()
    }

    private fun createTopics() {
        val topics = listOf(
            TopicBuilder.name("consume_produce_input").partitions(1),
            TopicBuilder.name("consume_produce_output").partitions(2),
            TopicBuilder.name("produce_consume_output").partitions(1),
            TopicBuilder.name("produce_consume_input").partitions(2),
            TopicBuilder.name("health_check").partitions(1)
                .config(TopicConfig.RETENTION_BYTES_CONFIG, (1024L * 1024L).toString()),
        ).map { it.build() }

        AdminClient.create(
            mapOf(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            )
        ).use { adminClient ->
            val actualTopics = adminClient.listTopics().names().get()
            if (!actualTopics.containsAll(topics.map(NewTopic::name))) {
                adminClient.createTopics(topics).all().get()
            }
        }
    }
}
