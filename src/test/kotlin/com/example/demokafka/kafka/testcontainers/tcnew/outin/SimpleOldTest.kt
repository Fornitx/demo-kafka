package com.example.demokafka.kafka.testcontainers.tcnew.outin

import com.example.demokafka.TestProfiles
import com.example.demokafka.kafka.services.ProduceAndConsumeKafkaService
import com.example.demokafka.kafka.services.ProduceAndConsumeKafkaServiceOldImpl
import com.example.demokafka.kafka.testcontainers.tcnew.AbstractNewTestcontainersKafkaTest
import com.example.demokafka.properties.PREFIX
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.RepeatedTest
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles

@SpringBootTest(
    properties = ["$PREFIX.kafka.out-in-new-template=false"]
)
@ActiveProfiles(TestProfiles.OUT_IN)
class SimpleOldTest : AbstractNewTestcontainersKafkaTest() {
    @Autowired
    private lateinit var service: ProduceAndConsumeKafkaService

    @RepeatedTest(5)
    fun test() = runTest {
        assertThat(service).isInstanceOf(ProduceAndConsumeKafkaServiceOldImpl::class.java)

        outInSimpleTest(service)
    }
}
