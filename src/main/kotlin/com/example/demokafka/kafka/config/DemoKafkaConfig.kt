package com.example.demokafka.kafka.config

import com.example.demokafka.kafka.actuator.KafkaHealthIndicator
import com.example.demokafka.kafka.metrics.DemoKafkaMetrics
import com.example.demokafka.kafka.model.DemoRequest
import com.example.demokafka.kafka.model.DemoResponse
import com.example.demokafka.kafka.services.ConsumeAndProduceKafkaService
import com.example.demokafka.kafka.services.ProduceAndConsumeKafkaService
import com.example.demokafka.properties.CustomKafkaProperties
import com.example.demokafka.properties.DemoProperties
import com.example.demokafka.properties.PREFIX
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.health.contributor.HealthIndicator
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.ContainerCustomizer
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.listener.*
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate

@Configuration
class DemoKafkaConfig(
    private val properties: DemoProperties,
    private val metrics: DemoKafkaMetrics,

    private val consumerFactory: ConsumerFactory<*, *>,
    private val producerFactory: ProducerFactory<*, *>,
) {
    @ConditionalOnProperty("$PREFIX.kafka.consume-produce.enabled", havingValue = "true")
    @Configuration
    inner class ConsumeProduceConfig {
        private val customKafkaProperties = properties.kafka.consumeProduce

        @Bean
        fun kleh(): KafkaListenerErrorHandler = KafkaListenerErrorHandler { consumerRecord, throwable ->
            println(123)
            throwable.printStackTrace()
        }

//        @Bean
        fun <K : Any, V : Any, C: AbstractMessageListenerContainer<K, V>> containerCustomizer(): ContainerCustomizer<K, V, C> =
            ContainerCustomizer { container ->
                container.commonErrorHandler = object : CommonErrorHandler {
                    override fun handleOne(
                        thrownException: Exception,
                        record: ConsumerRecord<*, *>,
                        consumer: Consumer<*, *>,
                        container: MessageListenerContainer,
                    ): Boolean {
                        return super.handleOne(thrownException, record, consumer, container)
                    }
                }
            }

        @Bean
        fun consumeAndProduceKafkaService(kafkaTemplate: KafkaTemplate<String, DemoResponse>): ConsumeAndProduceKafkaService =
            ConsumeAndProduceKafkaService(customKafkaProperties, kafkaTemplate, metrics)

        @ConditionalOnProperty("$PREFIX.kafka.consume-produce.health.enabled", havingValue = "true")
        @Bean
        fun kafkaHealthIndicator1(kafkaTemplate: KafkaTemplate<String, Long>): HealthIndicator =
            newKafkaHealthIndicator(customKafkaProperties, kafkaTemplate)
    }

    @ConditionalOnProperty("$PREFIX.kafka.produce-consume.enabled", havingValue = "true")
    @Configuration
    inner class ProduceConsumeConfig {
        private val customKafkaProperties = properties.kafka.produceConsume

        @Bean
        fun replyingKafkaTemplate(): ReplyingKafkaTemplate<String, DemoRequest, DemoResponse> {
            producerFactory as ProducerFactory<String, DemoRequest>
            consumerFactory as ConsumerFactory<String, DemoResponse>

            val containerProperties = ContainerProperties(customKafkaProperties.inputTopic)
            val replyContainer = KafkaMessageListenerContainer(consumerFactory, containerProperties)
            return ReplyingKafkaTemplate(producerFactory, replyContainer)
        }

        @Bean
        fun produceAndConsumeKafkaService(
            template: ReplyingKafkaTemplate<String, DemoRequest, DemoResponse>,
        ): ProduceAndConsumeKafkaService = ProduceAndConsumeKafkaService(customKafkaProperties, template, metrics)

        @ConditionalOnProperty("$PREFIX.kafka.produce-consume.health.enabled", havingValue = "true")
        @Bean
        fun kafkaHealthIndicator2(kafkaTemplate: KafkaTemplate<String, Long>): HealthIndicator =
            newKafkaHealthIndicator(customKafkaProperties, kafkaTemplate)
    }

//    private inline fun <reified K, V> KLogging.newKafkaConsumer(
//        customKafkaProperties: CustomKafkaProperties,
//        keyDeserializer: Deserializer<K>,
//        valueDeserializer: Deserializer<V>,
//    ): ReactiveKafkaConsumerTemplate<K, V> {
//        val receiverOptions = ReceiverOptions.create<K, V>(consumerFactory.configurationProperties)
//            .withKeyDeserializer(ErrorHandlingDeserializer(keyDeserializer))
//            .withValueDeserializer(ErrorHandlingDeserializer(valueDeserializer))
//            .subscription(listOf(customKafkaProperties.inputTopic))
//            .addAssignListener { partitions -> logger.info { "Assigned: $partitions" } }
//            .addRevokeListener { partitions -> logger.info { "Revoked: $partitions" } }
//
//        logger.info { "KafkaConsumer created for topic ${receiverOptions.subscriptionTopics()} on server ${receiverOptions.bootstrapServers()}" }
//
//        return ReactiveKafkaConsumerTemplate(receiverOptions)
//    }
//
//    private fun <K, V> KLogging.newKafkaProducer(
//        keySerializer: Serializer<K>,
//        valueSerializer: Serializer<V>,
//    ): ReactiveKafkaProducerTemplate<K, V> {
//        val senderOptions = SenderOptions.create<K, V>(producerFactory.configurationProperties)
//            .withKeySerializer(keySerializer)
//            .withValueSerializer(valueSerializer)
//
//        logger.info { "KafkaProducer created on server ${senderOptions.bootstrapServers()}" }
//
//        return ReactiveKafkaProducerTemplate(senderOptions)
//    }

    private fun newKafkaHealthIndicator(
        customKafkaProperties: CustomKafkaProperties,
        kafkaTemplate: KafkaTemplate<String, Long>,
    ): HealthIndicator {
        // TODO key = POD name
        return KafkaHealthIndicator(customKafkaProperties.health, kafkaTemplate)
    }
}
