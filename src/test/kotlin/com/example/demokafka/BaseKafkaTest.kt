package com.example.demokafka

import mu.KLogging
import mu.KotlinLogging
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName
import kotlin.reflect.jvm.jvmName

abstract class BaseKafkaTest {
    companion object : KLogging() {
        @JvmStatic
        protected val kafkaContainer: KafkaContainer = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.2.1"))
//            .withCreateContainerCmdModifier {
//                it.hostConfig!!.withPortBindings(
//                    PortBinding(
//                        Ports.Binding.bindPort(9092),
//                        ExposedPort(KafkaContainer.KAFKA_PORT)
//                    ),
////                    PortBinding(
////                        Ports.Binding.bindPort(KafkaContainer.ZOOKEEPER_PORT),
////                        ExposedPort(KafkaContainer.ZOOKEEPER_PORT)
////                    )
//                )
//            }
//    .withStartupTimeout(Duration.ofSeconds(10))

        init {
            kafkaContainer.start()

            System.setProperty("KAFKA_SERVER", kafkaContainer.bootstrapServers)

            logger.info { "\nKafka Container started on ${kafkaContainer.bootstrapServers}" }
        }
    }

    protected val log = KotlinLogging.logger(this::class.jvmName)
}
