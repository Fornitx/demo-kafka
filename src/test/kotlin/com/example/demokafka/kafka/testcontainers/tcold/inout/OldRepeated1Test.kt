package com.example.demokafka.kafka.testcontainers.tcold.inout

import com.example.demokafka.TestProfiles
import com.example.demokafka.kafka.testcontainers.tcold.AbstractOldTestcontainersKafkaTest
import org.junit.jupiter.api.RepeatedTest
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles

@SpringBootTest
@ActiveProfiles(TestProfiles.IN_OUT)
class OldRepeated1Test : AbstractOldTestcontainersKafkaTest() {
    @RepeatedTest(5)
    fun test() {
        inOutSimpleTest()
    }
}
