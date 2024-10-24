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
import com.example.demokafka.properties.DemoKafkaProperties
import com.example.demokafka.properties.PREFIX
import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.producer.ProducerConfig
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.actuate.health.ReactiveHealthIndicator
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.boot.ssl.SslBundles
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
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

private val log = KotlinLogging.logger {}

@Configuration
class DemoKafkaConfig(
    private val springKafkaProperties: KafkaProperties,
    private val properties: DemoKafkaProperties,
    private val sslBundles: ObjectProvider<SslBundles>,
) {
    @ConditionalOnProperty("$PREFIX.kafka.in-out.enabled", havingValue = "true")
    @Configuration
    inner class InOutConfig {
        private val kafkaProperties = properties.kafka.inOut

        @Bean
        fun kafkaConsumer1(): ReactiveKafkaConsumerTemplate<String, DemoRequest> =
            newKafkaConsumer(springKafkaProperties, kafkaProperties)

        @Bean
        fun kafkaProducer1(): ReactiveKafkaProducerTemplate<String, DemoResponse> =
            newKafkaProducer(springKafkaProperties, kafkaProperties)

        @Bean
        fun consumeAndProduceKafkaService(
            consumer: ReactiveKafkaConsumerTemplate<String, DemoRequest>,
            producer: ReactiveKafkaProducerTemplate<String, DemoResponse>,
            metrics: DemoKafkaMetrics,
        ): ConsumeAndProduceKafkaService = ConsumeAndProduceKafkaService(properties, consumer, producer, metrics)

        @ConditionalOnProperty("$PREFIX.kafka.in-out.health.enabled", havingValue = "true")
        @Bean
        fun kafkaHealthIndicator1(): ReactiveHealthIndicator =
            newKafkaHealthIndicator(springKafkaProperties, kafkaProperties)
    }

    @ConditionalOnProperty("$PREFIX.kafka.out-in.enabled", havingValue = "true")
    @Configuration
    inner class OutInConfig {
        private val kafkaProperties = properties.kafka.outIn

        @ConditionalOnProperty("$PREFIX.kafka.out-in-new-template", havingValue = "false")
        @Bean
        fun replyingKafkaOldTemplate2(): ReplyingKafkaTemplate<String, DemoRequest, DemoResponse> {
            val producerProperties =
                springKafkaProperties.buildProducerProperties(null) + kafkaProperties.buildProducerProperties(null)
            val consumerProperties =
                springKafkaProperties.buildConsumerProperties(null) + kafkaProperties.buildConsumerProperties(null)

            val producerFactory = DefaultKafkaProducerFactory<String, DemoRequest>(producerProperties)
            producerFactory.setValueSerializer(JsonSerializer<DemoRequest>().noTypeInfo())
            val consumerFactory = DefaultKafkaConsumerFactory<String, DemoResponse>(consumerProperties)
            consumerFactory.setValueDeserializer(JsonDeserializer(DemoResponse::class.java).ignoreTypeHeaders())

            val containerProperties = ContainerProperties(kafkaProperties.inputTopic)
            val replyContainer = KafkaMessageListenerContainer(consumerFactory, containerProperties)
            return ReplyingKafkaTemplate(producerFactory, replyContainer)
        }

        @ConditionalOnProperty("$PREFIX.kafka.out-in-new-template", havingValue = "true")
        @Bean
        fun replyingKafkaNewTemplate2(
            metrics: DemoKafkaMetrics,
        ): ReactiveReplyingKafkaTemplate<String, DemoRequest, DemoResponse> {
            val producerProperties = springKafkaProperties.buildProducerProperties(sslBundles.ifAvailable) +
                    kafkaProperties.buildProducerProperties(sslBundles.ifAvailable)
            val consumerProperties = springKafkaProperties.buildConsumerProperties(sslBundles.ifAvailable) +
                    kafkaProperties.buildConsumerProperties(sslBundles.ifAvailable)

            val senderOptions = SenderOptions.create<String, DemoRequest>(producerProperties)
                .withValueSerializer(JsonSerializer<DemoRequest>().noTypeInfo())
            val receiverOptions = ReceiverOptions.create<String, DemoResponse>(consumerProperties)
                .withValueDeserializer(JsonDeserializer(DemoResponse::class.java).ignoreTypeHeaders())
                .subscription(listOf(kafkaProperties.inputTopic))
                .addAssignListener { partitions -> log.info { "Assigned: $partitions" } }
                .addRevokeListener { partitions -> log.info { "Revoked: $partitions" } }

            val producer = ReactiveKafkaProducerTemplate(senderOptions)
            val consumer = ReactiveKafkaConsumerTemplate(receiverOptions)

            log.info { "KafkaProducer created on server ${senderOptions.bootstrapServers()}" }
            log.info { "KafkaConsumer created for topic '${receiverOptions.subscriptionTopics()}' on server ${receiverOptions.bootstrapServers()}" }

            return ReactiveReplyingKafkaTemplate(
                producer, consumer, metrics, properties.kafka.outInTimeout, kafkaProperties.inputTopic
            )
        }

        @Bean
        fun produceAndConsumeKafkaService(
            oldTemplate: ReplyingKafkaTemplate<String, DemoRequest, DemoResponse>?,
            newTemplate: ReactiveReplyingKafkaTemplate<String, DemoRequest, DemoResponse>?,
            metrics: DemoKafkaMetrics,
        ): ProduceAndConsumeKafkaService {
            return if (oldTemplate != null) {
                ProduceAndConsumeKafkaServiceOldImpl(properties, oldTemplate, metrics)
            } else if (newTemplate != null) {
                ProduceAndConsumeKafkaServiceNewImpl(properties, newTemplate)
            } else {
                throw RuntimeException("all templates are null")
            }
        }

        @ConditionalOnProperty("$PREFIX.kafka.out-in.health.enabled", havingValue = "true")
        @Bean
        fun kafkaHealthIndicator2(): ReactiveHealthIndicator =
            newKafkaHealthIndicator(springKafkaProperties, kafkaProperties)
    }

    private inline fun <reified T> newKafkaConsumer(
        springKafkaProperties: KafkaProperties,
        customKafkaProperties: CustomKafkaProperties,
    ): ReactiveKafkaConsumerTemplate<String, T> {
        val consumerProperties = springKafkaProperties.buildConsumerProperties(sslBundles.ifAvailable) +
                customKafkaProperties.buildConsumerProperties(sslBundles.ifAvailable)
        val receiverOptions = ReceiverOptions.create<String, T>(consumerProperties)
            .withValueDeserializer(ErrorHandlingDeserializer(JsonDeserializer(T::class.java).ignoreTypeHeaders()))
            .subscription(listOf(customKafkaProperties.inputTopic))
            .addAssignListener { partitions -> log.info { "Assigned: $partitions" } }
            .addRevokeListener { partitions -> log.info { "Revoked: $partitions" } }

        log.info { "KafkaConsumer created for topic '${receiverOptions.subscriptionTopics()}' on server '${receiverOptions.bootstrapServers()}'" }

        return ReactiveKafkaConsumerTemplate(receiverOptions)
    }

    private fun <T> newKafkaProducer(
        springKafkaProperties: KafkaProperties,
        customKafkaProperties: CustomKafkaProperties,
    ): ReactiveKafkaProducerTemplate<String, T> {
        val producerProperties = springKafkaProperties.buildProducerProperties(sslBundles.ifAvailable) +
                customKafkaProperties.buildProducerProperties(sslBundles.ifAvailable) +
                mapOf(ProducerConfig.MAX_BLOCK_MS_CONFIG to 5000)
        val senderOptions = SenderOptions.create<String, T>(producerProperties)
            .withValueSerializer(JsonSerializer<T>().noTypeInfo())

        log.info { "KafkaProducer created on server '${senderOptions.bootstrapServers()}'" }

        return ReactiveKafkaProducerTemplate(senderOptions)
    }

    private fun newKafkaHealthIndicator(
        springKafkaProperties: KafkaProperties,
        customKafkaProperties: CustomKafkaProperties,
    ): ReactiveHealthIndicator {
        val producerProperties = springKafkaProperties.buildProducerProperties(sslBundles.ifAvailable) +
                customKafkaProperties.buildProducerProperties(sslBundles.ifAvailable)
        val senderOptions = SenderOptions.create<Long, Long>(producerProperties)

        log.info { "KafkaHealthIndicator created for topic '${customKafkaProperties.health.topic}' on server '${senderOptions.bootstrapServers()}'" }

        return KafkaHealthIndicator(senderOptions, customKafkaProperties.health.topic)
    }
}
