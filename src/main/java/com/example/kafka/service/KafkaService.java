package com.example.kafka.service;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class KafkaService {
    private final ReactiveKafkaProducerTemplate<UUID, MyDto> producer;
    private final ReactiveKafkaConsumerTemplate<UUID, MyDto> consumer;

    @EventListener(ApplicationReadyEvent.class)
    public void initConsumer() {
        consumer.receiveAutoAck()
            .doOnNext(r -> System.out.printf("RECEIVE: %s%n", r))
            .doOnNext(r -> System.out.printf("RECEIVE: key = %s value = %s%n", r.key(), r.value()))
            .subscribe();

        var id = UUID.randomUUID();
        var value = new MyDto(id, Long.MAX_VALUE, "simple text", Boolean.TRUE, Instant.now());
//        producer.send(KafkaConfig.TOPIC, id, value).subscribe();
    }
}
