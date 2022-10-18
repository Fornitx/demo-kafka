package com.example.demokafka.kafka

import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConstructorBinding
import java.time.Duration

@ConfigurationProperties("demo", ignoreUnknownFields = false)
@ConstructorBinding
data class DemoKafkaProperties(val kafka: MyKafkaProperties) {
    class MyKafkaProperties : KafkaProperties() {
        lateinit var inputTopic: String
        lateinit var outputTopic: String
        var healthCheckTimeout: Duration? = null
    }
}
