package com.example.demokafka.kafka.config

import com.example.demokafka.kafka.ReactiveReplyingKafkaTemplate
import com.example.demokafka.kafka.actuator.KafkaHealthIndicator
import com.example.demokafka.kafka.metrics.DemoKafkaMetrics
import com.example.demokafka.kafka.model.DemoRequest
import com.example.demokafka.kafka.model.DemoResponse
import com.example.demokafka.kafka.services.ConsumeAndProduceKafkaService
import com.example.demokafka.kafka.services.ProduceAndConsumeKafkaService
import com.example.demokafka.kafka.services.ProduceAndConsumeKafkaServiceNewImpl
import com.example.demokafka.kafka.services.ProduceAndConsumeKafkaServiceOldImpl
import com.example.demokafka.properties.CustomKafkaProperties
import com.example.demokafka.properties.DemoProperties
import com.example.demokafka.properties.PREFIX
import com.example.demokafka.utils.typeRef
import com.fasterxml.jackson.databind.ObjectMapper
import io.github.oshai.kotlinlogging.KLogging
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.boot.actuate.health.ReactiveHealthIndicator
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.listener.KafkaMessageListenerContainer
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
import org.springframework.kafka.support.serializer.JsonDeserializer
import org.springframework.kafka.support.serializer.JsonSerializer
import reactor.kafka.receiver.ReceiverOptions
import reactor.kafka.sender.SenderOptions

@Configuration
class DemoKafkaConfig(
    private val objectMapper: ObjectMapper,

    private val consumerFactory: ConsumerFactory<*, *>,
    private val producerFactory: ProducerFactory<*, *>,

    private val properties: DemoProperties,
    private val metrics: DemoKafkaMetrics,
) {
    @ConditionalOnProperty("$PREFIX.kafka.consume-produce.enabled", havingValue = "true")
    @Configuration
    inner class ConsumeProduceConfig : KLogging() {
        private val customKafkaProperties = properties.kafka.consumeProduce

        @Bean
        fun kafkaConsumer1(): ReactiveKafkaConsumerTemplate<String, DemoRequest> = newKafkaConsumer(
            customKafkaProperties,
            StringDeserializer(),
            JsonDeserializer(typeRef<DemoRequest>(), objectMapper).ignoreTypeHeaders()
        )

        @Bean
        fun kafkaProducer1(): ReactiveKafkaProducerTemplate<String, DemoResponse> = newKafkaProducer(
            StringSerializer(),
            JsonSerializer<DemoResponse>(objectMapper).noTypeInfo()
        )

        @Bean
        fun consumeAndProduceKafkaService(): ConsumeAndProduceKafkaService =
            ConsumeAndProduceKafkaService(customKafkaProperties, kafkaConsumer1(), kafkaProducer1(), metrics)

        @ConditionalOnProperty("$PREFIX.kafka.consume-produce.health.enabled", havingValue = "true")
        @Bean
        fun kafkaHealthIndicator1(): ReactiveHealthIndicator = newKafkaHealthIndicator(customKafkaProperties)
    }

    @ConditionalOnProperty("$PREFIX.kafka.produce-consume.enabled", havingValue = "true")
    @Configuration
    inner class ProduceConsumeConfig : KLogging() {
        private val customKafkaProperties = properties.kafka.produceConsume

        @ConditionalOnProperty("$PREFIX.kafka.produce-consume-new-template", havingValue = "false")
        @Bean
        fun replyingKafkaTemplate(): ReplyingKafkaTemplate<String, DemoRequest, DemoResponse> {
            val producerFactory = DefaultKafkaProducerFactory<String, DemoRequest>(
                producerFactory.configurationProperties,
            ).apply {
                keySerializer = StringSerializer()
                valueSerializer = JsonSerializer<DemoRequest>(objectMapper).noTypeInfo()
            }
            val consumerFactory = DefaultKafkaConsumerFactory<String, DemoResponse>(
                consumerFactory.configurationProperties,
            ).apply {
                setKeyDeserializer(StringDeserializer())
                setValueDeserializer(
                    JsonDeserializer(typeRef<DemoResponse>(), objectMapper).ignoreTypeHeaders()
                )
            }

            val containerProperties = ContainerProperties(customKafkaProperties.inputTopic)
            val replyContainer = KafkaMessageListenerContainer(consumerFactory, containerProperties)
            return ReplyingKafkaTemplate(producerFactory, replyContainer)
        }

        @ConditionalOnProperty("$PREFIX.kafka.produce-consume-new-template", havingValue = "true")
        @Bean
        fun reactiveReplyingKafkaTemplate(): ReactiveReplyingKafkaTemplate<String, DemoRequest, DemoResponse> {
            val senderOptions = SenderOptions.create<String, DemoRequest>(producerFactory.configurationProperties)
                .withKeySerializer(StringSerializer())
                .withValueSerializer(JsonSerializer<DemoRequest>(objectMapper).noTypeInfo())
            val receiverOptions = ReceiverOptions.create<String, DemoResponse>(consumerFactory.configurationProperties)
                .withKeyDeserializer(StringDeserializer())
                .withValueDeserializer(JsonDeserializer(typeRef<DemoResponse>(), objectMapper).ignoreTypeHeaders())
                .subscription(listOf(customKafkaProperties.inputTopic))
                .addAssignListener { partitions -> logger.info { "Assigned: $partitions" } }
                .addRevokeListener { partitions -> logger.info { "Revoked: $partitions" } }

            val producer = ReactiveKafkaProducerTemplate(senderOptions)
            val consumer = ReactiveKafkaConsumerTemplate(receiverOptions)

            logger.info { "KafkaProducer created on server ${senderOptions.bootstrapServers()}" }
            logger.info { "KafkaConsumer created for topic '${receiverOptions.subscriptionTopics()}' on server ${receiverOptions.bootstrapServers()}" }

            return ReactiveReplyingKafkaTemplate(
                producer, consumer, metrics, properties.kafka.produceConsumeTimeout, customKafkaProperties.inputTopic
            )
        }

        @Bean
        fun produceAndConsumeKafkaService(
            oldTemplate: ReplyingKafkaTemplate<String, DemoRequest, DemoResponse>?,
            newTemplate: ReactiveReplyingKafkaTemplate<String, DemoRequest, DemoResponse>?,
        ): ProduceAndConsumeKafkaService {
            return if (oldTemplate != null) {
                logger.debug { "using old template" }
                ProduceAndConsumeKafkaServiceOldImpl(properties, oldTemplate, metrics)
            } else if (newTemplate != null) {
                logger.debug { "using new template" }
                ProduceAndConsumeKafkaServiceNewImpl(properties, newTemplate)
            } else {
                throw RuntimeException("all templates are null")
            }
        }

        @ConditionalOnProperty("$PREFIX.kafka.produce-consume.health.enabled", havingValue = "true")
        @Bean
        fun kafkaHealthIndicator2(): ReactiveHealthIndicator =
            newKafkaHealthIndicator(customKafkaProperties)
    }

    private inline fun <reified K, V> KLogging.newKafkaConsumer(
        customKafkaProperties: CustomKafkaProperties,
        keyDeserializer: Deserializer<K>,
        valueDeserializer: Deserializer<V>,
    ): ReactiveKafkaConsumerTemplate<K, V> {
        val receiverOptions = ReceiverOptions.create<K, V>(consumerFactory.configurationProperties)
            .withKeyDeserializer(ErrorHandlingDeserializer(keyDeserializer))
            .withValueDeserializer(ErrorHandlingDeserializer(valueDeserializer))
            .subscription(listOf(customKafkaProperties.inputTopic))
            .addAssignListener { partitions -> logger.info { "Assigned: $partitions" } }
            .addRevokeListener { partitions -> logger.info { "Revoked: $partitions" } }

        logger.info { "KafkaConsumer created for topic ${receiverOptions.subscriptionTopics()} on server ${receiverOptions.bootstrapServers()}" }

        return ReactiveKafkaConsumerTemplate(receiverOptions)
    }

    private fun <K, V> KLogging.newKafkaProducer(
        keySerializer: Serializer<K>,
        valueSerializer: Serializer<V>,
    ): ReactiveKafkaProducerTemplate<K, V> {
        val senderOptions = SenderOptions.create<K, V>(producerFactory.configurationProperties)
            .withKeySerializer(keySerializer)
            .withValueSerializer(valueSerializer)

        logger.info { "KafkaProducer created on server ${senderOptions.bootstrapServers()}" }

        return ReactiveKafkaProducerTemplate(senderOptions)
    }

    private fun KLogging.newKafkaHealthIndicator(
        customKafkaProperties: CustomKafkaProperties,
    ): ReactiveHealthIndicator {
        // TODO key = POD name
        val senderOptions = SenderOptions.create<Long, Long>(producerFactory.configurationProperties)

        logger.info { "KafkaHealthIndicator created for topic '${customKafkaProperties.health.topic}' on server '${senderOptions.bootstrapServers()}'" }

        return KafkaHealthIndicator(ReactiveKafkaProducerTemplate(senderOptions), customKafkaProperties.health.topic)
    }
}
