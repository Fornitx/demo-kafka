package com.example.demokafka.kafka.indicator

import com.example.demokafka.kafka.embedded.AbstractEmbeddedKafkaTest
import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.web.reactive.server.WebTestClient
import org.springframework.test.web.reactive.server.expectBody

//@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
//@AutoConfigureWebTestClient
//@Disabled
//class KafkaHealthIndicatorTest : AbstractEmbeddedKafkaTest() {
//    @Autowired
//    private lateinit var client: WebTestClient
//
//    @Autowired
//    private lateinit var objectMapper: ObjectMapper
//
//    @Test
//    fun test() {
//        val body = client.get()
//            .uri("/actuator/health")
//            .exchange()
//            .expectStatus()
//            .isOk
//            .expectBody<String>()
//            .returnResult()
//            .responseBody
//
//        log.info { "body = $body" }
//
//        val json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(objectMapper.readTree(body))
//        log.info { "json = \n$json" }
//    }
//}
