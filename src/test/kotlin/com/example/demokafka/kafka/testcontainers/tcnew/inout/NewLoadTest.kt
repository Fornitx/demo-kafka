package com.example.demokafka.kafka.testcontainers.tcnew.inout

import com.example.demokafka.TestProfiles
import com.example.demokafka.kafka.testcontainers.tcnew.AbstractNewTestcontainersKafkaTest
import org.junit.jupiter.api.RepeatedTest
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles

@SpringBootTest
@ActiveProfiles(TestProfiles.IN_OUT)
class NewLoadTest : AbstractNewTestcontainersKafkaTest() {
    @RepeatedTest(5)
    fun test() {
        inOutLoadTest()
    }
}
