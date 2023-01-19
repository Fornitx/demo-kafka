package com.example.demokafka

import org.junit.jupiter.api.Test
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient
import org.springframework.boot.test.context.SpringBootTest
import java.util.concurrent.TimeUnit

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@AutoConfigureWebTestClient
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
