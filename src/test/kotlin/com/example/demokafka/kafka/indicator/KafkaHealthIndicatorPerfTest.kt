package com.example.demokafka.kafka.indicator

import com.example.demokafka.kafka.AbstractKafkaTest
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import java.util.concurrent.TimeUnit

@SpringBootTest
@Disabled
class KafkaHealthIndicatorPerfTest : AbstractKafkaTest() {
    @Test
    fun test() {
        val runtime = Runtime.getRuntime()
        while (true) {
            println("maxMemory = ${runtime.maxMemory()}, freeMemory = ${runtime.freeMemory()}, totalMemory = ${runtime.totalMemory()}")
            TimeUnit.MILLISECONDS.sleep(500)
        }
    }
}
