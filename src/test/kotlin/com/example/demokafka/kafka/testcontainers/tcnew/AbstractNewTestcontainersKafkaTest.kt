package com.example.demokafka.kafka.testcontainers.tcnew

import com.example.demokafka.TestProfiles.TESTCONTAINERS
import com.example.demokafka.kafka.AbstractKafkaTest
import com.example.demokafka.kafka.testcontainers.TestcontainersHelper
import org.springframework.test.context.ActiveProfiles
import org.testcontainers.kafka.KafkaContainer

@ActiveProfiles(TESTCONTAINERS)
abstract class AbstractNewTestcontainersKafkaTest : AbstractKafkaTest() {
    override val bootstrapServers: String
        get() = kafkaContainer.bootstrapServers

    companion object {
        protected val kafkaContainer: KafkaContainer = TestcontainersHelper.KAFKA_CONTAINER
    }
}
