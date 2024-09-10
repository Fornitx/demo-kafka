package com.example.demokafka.kafka.config

import com.example.demokafka.kafka.actuator.KafkaHealthIndicator
import com.example.demokafka.kafka.metrics.DemoKafkaMetrics
import com.example.demokafka.kafka.model.DemoRequest
import com.example.demokafka.kafka.model.DemoResponse
import com.example.demokafka.kafka.services.ConsumeAndProduceKafkaService
import com.example.demokafka.kafka.services.ProduceAndConsumeKafkaService
import com.example.demokafka.properties.CustomKafkaProperties
import com.example.demokafka.properties.DemoKafkaProperties
import com.example.demokafka.properties.PREFIX
import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.common.serialization.LongSerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.boot.actuate.health.ReactiveHealthIndicator
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
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
) {
    @ConditionalOnProperty("$PREFIX.in-out-kafka.enabled", havingValue = "true", matchIfMissing = false)
    @Configuration
    inner class InOutConfig {
        private val kafkaProperties = properties.inOutKafka

//        @ConditionalOnMissingBean
//        @Bean
//        fun kafkaAdmin(): KafkaAdmin = newKafkaAdmin(springKafkaProperties, kafkaProperties)
//
//        @Bean
//        fun inTopic1(): NewTopic = newTopic(kafkaProperties.inputTopic, 4)
//
//        @Bean
//        fun outTopic1(): NewTopic = newTopic(kafkaProperties.outputTopic)

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

        @ConditionalOnProperty("$PREFIX.in-out-kafka.health.enabled", havingValue = "true", matchIfMissing = false)
        @Configuration
        inner class InOutHealthConfig {
//            @Bean
//            fun healthCheckTopic1(): NewTopic = newTopic(kafkaProperties.health.topic)

            @Bean
            fun kafkaHealthIndicator1(): ReactiveHealthIndicator =
                newKafkaHealthIndicator(springKafkaProperties, kafkaProperties)
        }
    }

    @ConditionalOnProperty("$PREFIX.out-in-kafka.enabled", havingValue = "true", matchIfMissing = false)
    @Configuration
    inner class OutInConfig {
        private val kafkaProperties = properties.outInKafka

//        @ConditionalOnMissingBean
//        @Bean
//        fun kafkaAdmin(): KafkaAdmin = newKafkaAdmin(springKafkaProperties, kafkaProperties)
//
//        @Bean
//        fun inTopic2(): NewTopic = newTopic(kafkaProperties.inputTopic, 4)
//
//        @Bean
//        fun outTopic2(): NewTopic = newTopic(kafkaProperties.outputTopic)

        @Bean
        fun replyingKafkaTemplate2(): ReplyingKafkaTemplate<String, DemoRequest, DemoResponse> {
            val producerProperties =
                springKafkaProperties.buildProducerProperties(null) + kafkaProperties.buildProducerProperties(null)
            val consumerProperties =
                springKafkaProperties.buildConsumerProperties(null) + kafkaProperties.buildConsumerProperties(null)

            val producerFactory = DefaultKafkaProducerFactory<String, DemoRequest>(producerProperties)
            val consumerFactory = DefaultKafkaConsumerFactory<String, DemoResponse>(consumerProperties)
            val containerProperties = ContainerProperties(kafkaProperties.inputTopic)
            val replyContainer = KafkaMessageListenerContainer(consumerFactory, containerProperties)
            return ReplyingKafkaTemplate(producerFactory, replyContainer)
        }

        @Bean
        fun consumeAndProduceKafkaService(
            template: ReplyingKafkaTemplate<String, DemoRequest, DemoResponse>,
            metrics: DemoKafkaMetrics,
        ): ProduceAndConsumeKafkaService = ProduceAndConsumeKafkaService(properties, template, metrics)

        @ConditionalOnProperty("$PREFIX.out-in-kafka.health.enabled", havingValue = "true", matchIfMissing = false)
        @Configuration
        inner class OutInHealthConfig {
//            @Bean
//            fun healthCheckTopic2(): NewTopic = newTopic(kafkaProperties.health.topic)

            @Bean
            fun kafkaHealthIndicator2(): ReactiveHealthIndicator =
                newKafkaHealthIndicator(springKafkaProperties, kafkaProperties)
        }
    }

    companion object {
//        private fun newKafkaAdmin(
//            springKafkaProperties: KafkaProperties,
//            customKafkaProperties: CustomKafkaProperties,
//        ): KafkaAdmin {
//            log.info { "KafkaAdmin created" }
//            return KafkaAdmin(
//                springKafkaProperties.buildAdminProperties(null) + customKafkaProperties.buildAdminProperties(null)
//            )
//        }
//
//        private fun newTopic(name: String, partitions: Int = 1): NewTopic {
//            log.info { "Topic $name created" }
//            return TopicBuilder.name(name).partitions(partitions).build()
//        }

        private inline fun <reified T> newKafkaConsumer(
            springKafkaProperties: KafkaProperties,
            customKafkaProperties: CustomKafkaProperties,
        ): ReactiveKafkaConsumerTemplate<String, T> {
            val consumerProperties =
                springKafkaProperties.buildConsumerProperties(null) + customKafkaProperties.buildConsumerProperties(null)

            val receiverOptions = ReceiverOptions.create<String, T>(consumerProperties)
                .withKeyDeserializer(StringDeserializer())
                .withValueDeserializer(ErrorHandlingDeserializer(JsonDeserializer(T::class.java).ignoreTypeHeaders()))
                .subscription(listOf(customKafkaProperties.inputTopic))

            log.info { "KafkaConsumer created for topic '${receiverOptions.subscriptionTopics()}' on server ${receiverOptions.bootstrapServers()}" }

            return ReactiveKafkaConsumerTemplate(receiverOptions)
        }

        private fun <T> newKafkaProducer(
            springKafkaProperties: KafkaProperties,
            customKafkaProperties: CustomKafkaProperties,
        ): ReactiveKafkaProducerTemplate<String, T> {
            val producerProperties =
                springKafkaProperties.buildProducerProperties(null) + customKafkaProperties.buildProducerProperties(null)
            val senderOptions = SenderOptions.create<String, T>(producerProperties)
                .withKeySerializer(StringSerializer())
                .withValueSerializer(JsonSerializer<T>().noTypeInfo())

            log.info { "KafkaProducer created on server ${senderOptions.bootstrapServers()}" }

            return ReactiveKafkaProducerTemplate(senderOptions)
        }

        private fun newKafkaHealthIndicator(
            springKafkaProperties: KafkaProperties,
            customKafkaProperties: CustomKafkaProperties,
        ): ReactiveHealthIndicator {
            val producerProperties =
                springKafkaProperties.buildProducerProperties(null) + customKafkaProperties.buildProducerProperties(null)
            val senderOptions = SenderOptions.create<Long, Long>(producerProperties)
                .withKeySerializer(LongSerializer())
                .withValueSerializer(LongSerializer())

            log.info { "KafkaHealthIndicator created for topic '${customKafkaProperties.health.topic}' on server ${senderOptions.bootstrapServers()}" }

            return KafkaHealthIndicator(senderOptions, customKafkaProperties.health.topic)
        }
    }
}
