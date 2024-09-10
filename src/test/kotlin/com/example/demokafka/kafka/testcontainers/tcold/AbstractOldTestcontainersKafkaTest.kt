package com.example.demokafka.kafka.testcontainers.tcold

import com.example.demokafka.TestProfiles.TESTCONTAINERS
import com.example.demokafka.kafka.AbstractKafkaTest
import com.example.demokafka.kafka.testcontainers.TestcontainersHelper
import org.springframework.test.context.ActiveProfiles
import org.testcontainers.containers.KafkaContainer

@ActiveProfiles(TESTCONTAINERS)
abstract class AbstractOldTestcontainersKafkaTest : AbstractKafkaTest() {
    override val bootstrapServers: String
        get() = kafkaContainer.bootstrapServers

    companion object {
        protected val kafkaContainer: KafkaContainer = TestcontainersHelper.KAFKA_CONTAINER_OLD
    }
}
