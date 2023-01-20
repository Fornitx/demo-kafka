package com.example.demokafka.kafka

import com.example.demokafka.kafka.dto.DemoRequest
import com.fasterxml.jackson.databind.ObjectMapper
import mu.KotlinLogging
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
import org.springframework.kafka.support.serializer.JsonDeserializer
import reactor.kafka.receiver.ReceiverOptions
import reactor.kafka.sender.SenderOptions

private val log = KotlinLogging.logger {}

@Configuration
class DemoKafkaConfig(private val properties: DemoKafkaProperties) {
    @Bean
    fun kafkaConsumer(): ReactiveKafkaConsumerTemplate<String, DemoRequest> {
        val consumerProperties = properties.kafka.buildConsumerProperties()
        val receiverOptions = ReceiverOptions.create<String, DemoRequest>(consumerProperties)
            .withKeyDeserializer(StringDeserializer())
            .withValueDeserializer(ErrorHandlingDeserializer(JsonDeserializer(DemoRequest::class.java)))
            .subscription(listOf(properties.kafka.inputTopic))

        log.info { "\nKafkaConsumer created for topic '${properties.kafka.inputTopic}' on server ${properties.kafka.bootstrapServers}" }

        return ReactiveKafkaConsumerTemplate(receiverOptions)
    }

    @Bean
    fun kafkaProducer(): ReactiveKafkaProducerTemplate<String, String> {
        val producerProperties = properties.kafka.buildProducerProperties()
        val senderOptions = SenderOptions.create<String, String>(producerProperties)
            .withKeySerializer(StringSerializer())
            .withValueSerializer(StringSerializer())

        log.info { "\nKafkaProducer created for topic '${properties.kafka.outputTopic}' on server ${properties.kafka.bootstrapServers}" }

        return ReactiveKafkaProducerTemplate(senderOptions)
    }

    @Bean
    fun kafkaService(objectMapper: ObjectMapper): DemoKafkaService {
        return DemoKafkaService(properties, objectMapper, kafkaConsumer(), kafkaProducer())
    }

    //    @Bean
    fun kafkaHealthIndicator(): KafkaHealthIndicator {
        log.info { "\nKafkaHealthIndicator created for topic '${properties.kafka.inputTopic}' on server ${properties.kafka.bootstrapServers}" }

        return KafkaHealthIndicator(properties.kafka)
    }
}
