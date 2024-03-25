package com.example.demokafka.properties

import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration

const val PREFIX = "demo"

@ConfigurationProperties(PREFIX, ignoreUnknownFields = false)
data class DemoKafkaProperties(val kafka: MyKafkaProperties) {
}

class MyKafkaProperties : KafkaProperties() {
    lateinit var inputTopic: String
    //        lateinit var outputTopic: String
    var healthCheckInterval: Duration? = null
    var healthCheckTimeout: Duration? = null
}
