package com.example.demokafka.kafka

import com.example.demokafka.kafka.dto.DemoRequest
import com.example.demokafka.kafka.dto.DemoResponse
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate
import org.springframework.kafka.support.serializer.JsonDeserializer
import org.springframework.kafka.support.serializer.JsonSerializer
import reactor.kafka.receiver.ReceiverOptions
import reactor.kafka.sender.SenderOptions

@Configuration
class DemoKafkaConfig(private val properties: DemoKafkaProperties) {
    @Bean
    fun kafkaConsumer(): ReactiveKafkaConsumerTemplate<String, DemoRequest> {
        val consumerProperties = properties.kafka.buildConsumerProperties()
        val receiverOptions = ReceiverOptions.create<String, DemoRequest>(consumerProperties)
            .withKeyDeserializer(StringDeserializer())
            .withValueDeserializer(JsonDeserializer<DemoRequest>().trustedPackages("*"))
            .subscription(listOf(properties.kafka.inputTopic))
        return ReactiveKafkaConsumerTemplate(receiverOptions)
    }

    @Bean
    fun kafkaProducer(): ReactiveKafkaProducerTemplate<String, DemoResponse> {
        val producerProperties = properties.kafka.buildProducerProperties()
        val senderOptions = SenderOptions.create<String, DemoResponse>(producerProperties)
            .withKeySerializer(StringSerializer())
            .withValueSerializer(JsonSerializer())
        return ReactiveKafkaProducerTemplate(senderOptions)
    }

    @Bean
    fun demoKafkaService(): DemoKafkaService {
        return DemoKafkaService(properties, kafkaConsumer(), kafkaProducer())
    }
}
