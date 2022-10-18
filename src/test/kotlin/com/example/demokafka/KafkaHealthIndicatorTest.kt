package com.example.demokafka

import com.example.demokafka.kafka.DemoKafkaProperties
import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.web.reactive.server.WebTestClient
import org.springframework.test.web.reactive.server.expectBody
import java.util.concurrent.TimeUnit

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@AutoConfigureWebTestClient
class KafkaHealthIndicatorTest : BaseKafkaTest() {
    @Autowired
    private lateinit var properties: DemoKafkaProperties

    @Autowired
    private lateinit var client: WebTestClient

    @Autowired
    private lateinit var objectMapper: ObjectMapper

    @Test
    fun test() {
        val body = client.get()
            .uri("/actuator/health/kafka")
            .exchange()
            .expectStatus().isOk
            .expectBody<String>()
            .returnResult()
            .responseBody

        log.info { "body = $body" }

        val json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(objectMapper.readTree(body))
        log.info { "json = \n$json" }

        TimeUnit.MINUTES.sleep(5)
    }
}