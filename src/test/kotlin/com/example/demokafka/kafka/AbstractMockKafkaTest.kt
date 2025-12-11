package com.example.demokafka.kafka

import com.example.demokafka.AbstractMetricsTest
import org.springframework.boot.autoconfigure.ImportAutoConfiguration
import org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.test.context.bean.override.mockito.MockitoBean

@ImportAutoConfiguration(exclude = [KafkaAutoConfiguration::class])
abstract class AbstractMockKafkaTest : AbstractMetricsTest() {
    @MockitoBean
    private lateinit var consumerFactory: ConsumerFactory<*, *>

    @MockitoBean
    private lateinit var producerFactory: ProducerFactory<*, *>

    @MockitoBean
    protected lateinit var mockedKafkaTemplate: KafkaTemplate<*, *>
}