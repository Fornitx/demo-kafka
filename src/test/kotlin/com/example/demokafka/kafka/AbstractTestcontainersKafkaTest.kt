package com.example.demokafka.kafka

import com.example.demokafka.utils.DemoKafkaContainer
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.lifecycle.Startables

abstract class AbstractTestcontainersKafkaTest : AbstractKafkaTest() {
    override val bootstrapServers: String
        get() = kafkaContainer.bootstrapServers

    companion object {
        @JvmStatic
        protected val kafkaContainer: KafkaContainer = DemoKafkaContainer

        init {
            Startables.deepStart(kafkaContainer).join()
        }
    }
}
