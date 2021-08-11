package com.example.kafka.service;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.UUIDDeserializer;
import org.apache.kafka.common.serialization.UUIDSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.SenderOptions;

import java.util.List;
import java.util.UUID;

@Configuration
public class KafkaConfig {
    public static final String TOPIC = "MyTopic";

    @Bean
    public ReactiveKafkaConsumerTemplate<UUID, MyDto> reactiveKafkaConsumerTemplate() {
        var receiverOptions = ReceiverOptions.<UUID, MyDto>create()
            .consumerProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
            .consumerProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, UUIDDeserializer.class)
            .consumerProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class)
            .consumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            .consumerProperty(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString())
            .consumerProperty(JsonDeserializer.TRUSTED_PACKAGES, "*")
            .subscription(List.of(TOPIC));
        return new ReactiveKafkaConsumerTemplate<>(receiverOptions);
    }

    @Bean
    public ReactiveKafkaProducerTemplate<UUID, MyDto> reactiveKafkaProducerTemplate() {
        var senderOptions = SenderOptions.<UUID, MyDto>create()
            .producerProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
            .producerProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, UUIDSerializer.class)
            .producerProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new ReactiveKafkaProducerTemplate<>(senderOptions);
    }
}
