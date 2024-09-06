package com.example.demokafka.kafka.indicator

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
