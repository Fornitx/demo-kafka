package com.example.demokafka.kafka.profiles

import com.example.demokafka.AbstractProfileTest
import com.example.demokafka.TestProfiles
import com.example.demokafka.kafka.actuator.KafkaHealthIndicator
import com.example.demokafka.kafka.services.ConsumeAndProduceKafkaService
import com.example.demokafka.kafka.services.ProduceAndConsumeKafkaService
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate

class KafkaProfileTest : AbstractProfileTest() {
    @Test
    fun testConsumeProduce() {
        contextRunner().withProfiles(TestProfiles.CONSUME_PRODUCE).runLogging {
            assertThat(it).hasNotFailed()
                .hasSingleBean(ConsumeAndProduceKafkaService::class.java)

                .doesNotHaveBean(ReplyingKafkaTemplate::class.java)
                .doesNotHaveBean(ProduceAndConsumeKafkaService::class.java)

                .doesNotHaveBean(KafkaHealthIndicator::class.java)
        }
    }

    @Test
    fun testProduceConsume() {
        contextRunner().withProfiles(TestProfiles.PRODUCE_CONSUME).runLogging {
            assertThat(it).hasNotFailed()
                .doesNotHaveBean(ConsumeAndProduceKafkaService::class.java)

                .hasSingleBean(ReplyingKafkaTemplate::class.java)
                .hasSingleBean(ProduceAndConsumeKafkaService::class.java)

                .doesNotHaveBean(KafkaHealthIndicator::class.java)
        }
    }
}