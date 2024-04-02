package com.example.demokafka.kafka

import com.example.demokafka.kafka.model.DemoRequest
import com.example.demokafka.kafka.model.DemoResponse
import com.example.demokafka.properties.DemoKafkaProperties
import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.TopicBuilder
import org.springframework.kafka.core.KafkaAdmin
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
import org.springframework.kafka.support.serializer.JsonDeserializer
import org.springframework.kafka.support.serializer.JsonSerializer
import reactor.kafka.receiver.ReceiverOptions
import reactor.kafka.sender.SenderOptions

private val log = KotlinLogging.logger {}

@Configuration
class DemoKafkaConfig(
    private val springKafkaProperties: KafkaProperties,
    private val properties: DemoKafkaProperties
) {
    @Bean
    fun kafkaAdmin(): KafkaAdmin =
        KafkaAdmin(mapOf(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to properties.kafka.bootstrapServers))

    @Bean
    fun inTopic(): NewTopic = TopicBuilder.name(properties.kafka.inputTopic).partitions(4).build()

    @Bean
    fun outTopic(): NewTopic = TopicBuilder.name(properties.kafka.outputTopic).partitions(1).build()

    @Bean
    fun kafkaConsumer(): ReactiveKafkaConsumerTemplate<String, DemoRequest> {
        val consumerProperties =
            springKafkaProperties.buildConsumerProperties(null) + properties.kafka.buildConsumerProperties(null)
        val receiverOptions = ReceiverOptions.create<String, DemoRequest>(consumerProperties)
            .withKeyDeserializer(StringDeserializer())
            .withValueDeserializer(ErrorHandlingDeserializer(JsonDeserializer(DemoRequest::class.java).ignoreTypeHeaders()))
            .subscription(listOf(properties.kafka.inputTopic))

        log.info { "KafkaConsumer created for topic '${properties.kafka.inputTopic}' on server ${properties.kafka.bootstrapServers}" }

        return ReactiveKafkaConsumerTemplate(receiverOptions)
    }

    @Bean
    fun kafkaProducer(): ReactiveKafkaProducerTemplate<String, DemoResponse> {
        val producerProperties =
            springKafkaProperties.buildConsumerProperties(null) + properties.kafka.buildProducerProperties(null)
        val senderOptions = SenderOptions.create<String, DemoResponse>(producerProperties)
            .withKeySerializer(StringSerializer())
            .withValueSerializer(JsonSerializer<DemoResponse>().noTypeInfo())

        log.info { "KafkaProducer created on server ${properties.kafka.bootstrapServers}" }

        return ReactiveKafkaProducerTemplate(senderOptions)
    }

    @Bean
    fun kafkaService(metrics: DemoKafkaMetrics): DemoKafkaService {
        return DemoKafkaService(properties, kafkaConsumer(), kafkaProducer(), metrics)
    }

    //    @Bean
    fun kafkaHealthIndicator(): KafkaHealthIndicator {
        log.info { "KafkaHealthIndicator created for topic '${properties.kafka.inputTopic}' on server ${properties.kafka.bootstrapServers}" }

        return KafkaHealthIndicator(springKafkaProperties, properties.kafka)
    }
}
