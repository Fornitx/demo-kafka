package com.example.demokafka.kafka.embedded.inout

import com.example.demokafka.TestProfiles
import com.example.demokafka.kafka.embedded.AbstractEmbeddedKafkaTest
import org.junit.jupiter.api.RepeatedTest
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles

@SpringBootTest
@ActiveProfiles(TestProfiles.IN_OUT)
class EmbRepeated1Test : AbstractEmbeddedKafkaTest() {
    @RepeatedTest(5)
    fun test() {
        inOutSimpleTest()
    }
}
