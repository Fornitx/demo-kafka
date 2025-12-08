package com.example.demokafka.kafka.outin

import com.example.demokafka.TestProfiles
import com.example.demokafka.kafka.AbstractTestcontainersKafkaTest
import com.example.demokafka.kafka.services.ProduceAndConsumeKafkaService
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.RepeatedTest
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles

@SpringBootTest
@ActiveProfiles(TestProfiles.PRODUCE_CONSUME)
class LoadTest: AbstractTestcontainersKafkaTest() {
    @Autowired
    private lateinit var service: ProduceAndConsumeKafkaService

    @RepeatedTest(1)
    fun test() = runTest {
//        assertThat(service).isInstanceOf(ProduceAndConsumeKafkaServiceNewImpl::class.java)

        // TODO
//        outInLoadTest(service)
    }
}
