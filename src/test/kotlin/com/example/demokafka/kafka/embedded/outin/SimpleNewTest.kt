package com.example.demokafka.kafka.embedded.outin

import com.example.demokafka.TestProfiles
import com.example.demokafka.kafka.embedded.AbstractEmbeddedKafkaTest
import com.example.demokafka.kafka.services.ProduceAndConsumeKafkaService
import com.example.demokafka.kafka.services.ProduceAndConsumeKafkaServiceNewImpl
import com.example.demokafka.properties.PREFIX
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.RepeatedTest
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles

@SpringBootTest(
    properties = ["$PREFIX.kafka.out-in-new-template=true"]
)
@ActiveProfiles(TestProfiles.OUT_IN)
class SimpleNewTest : AbstractEmbeddedKafkaTest() {
    @Autowired
    private lateinit var service: ProduceAndConsumeKafkaService

    @RepeatedTest(5)
    fun test() = runTest {
        assertThat(service).isInstanceOf(ProduceAndConsumeKafkaServiceNewImpl::class.java)

        outInSimpleTest(service)
    }
}