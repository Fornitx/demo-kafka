package com.example.demokafka

import com.example.demokafka.kafka.dto.DemoResponse
import com.github.dockerjava.api.model.ExposedPort
import com.github.dockerjava.api.model.PortBinding
import com.github.dockerjava.api.model.Ports
import mu.KLogging
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.serializer.JsonDeserializer
import org.springframework.kafka.test.utils.KafkaTestUtils
import org.springframework.test.annotation.DirtiesContext
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName
import java.time.Duration
import kotlin.reflect.jvm.jvmName

@DirtiesContext
abstract class AbstractKafkaTest {
    companion object : KLogging() {
        @JvmStatic
        protected val kafkaContainer: KafkaContainer =
            KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.2.1"))
                .withCreateContainerCmdModifier {
                    it.hostConfig!!.withPortBindings(
                        PortBinding(
                            Ports.Binding.bindPort(KafkaContainer.KAFKA_PORT),
                            ExposedPort(KafkaContainer.KAFKA_PORT)
                        ),
                        PortBinding(
                            Ports.Binding.bindPort(KafkaContainer.ZOOKEEPER_PORT),
                            ExposedPort(KafkaContainer.ZOOKEEPER_PORT)
                        )
                    )
                }.withReuse(true)

        init {
            kafkaContainer.start()

            System.setProperty("KAFKA_SERVER", kafkaContainer.bootstrapServers)

            logger.info { "\nKafka Container started on ${kafkaContainer.bootstrapServers}" }
        }

        fun <K, V> consume(
            consumerFactory: ConsumerFactory<K, V>,
            topic: String,
            timeout: Duration,
            minRecords: Int,
        ): ConsumerRecords<K, V> {
            return consumerFactory.createConsumer().use { consumer ->
                consumer.subscribe(listOf(topic))
                KafkaTestUtils.getRecords(consumer, timeout, minRecords)
            }
        }
    }

    protected val log = KotlinLogging.logger(this::class.jvmName)

    protected val template by lazy {
        KafkaTemplate(
            DefaultKafkaProducerFactory(
                mapOf(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaContainer.bootstrapServers),
                StringSerializer(),
                StringSerializer()
            )
        )
    }
    protected val consumerFactory by lazy {
        DefaultKafkaConsumerFactory(
            mapOf(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaContainer.bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG to AbstractKafkaTest::class.simpleName,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ),
            StringDeserializer(),
            JsonDeserializer(DemoResponse::class.java)
        )
    }
}
