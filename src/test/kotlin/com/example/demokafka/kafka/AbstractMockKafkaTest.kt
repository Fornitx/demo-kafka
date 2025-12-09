package com.example.demokafka.kafka

import com.example.demokafka.AbstractMetricsTest
import com.example.demokafka.TestProfiles
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.bean.override.mockito.MockitoBean

@ActiveProfiles(TestProfiles.MOCK_KAFKA)
abstract class AbstractMockKafkaTest : AbstractMetricsTest() {
    @MockitoBean
    private lateinit var consumerFactory: ConsumerFactory<*, *>

    @MockitoBean
    private lateinit var producerFactory: ProducerFactory<*, *>

    @MockitoBean
    protected lateinit var kafkaTemplate: KafkaTemplate<*, *>
}