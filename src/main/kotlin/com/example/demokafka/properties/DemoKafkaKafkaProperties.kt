package com.example.demokafka.properties

import jakarta.validation.constraints.NotBlank
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import java.time.Duration

data class DemoKafkaKafkaProperties(
    val inOut: CustomKafkaProperties,
    val outIn: CustomKafkaProperties,
    val outInTimeout: Duration,
    val outInNewTemplate: Boolean = false,
)

class CustomKafkaProperties : KafkaProperties() {
    var enabled: Boolean = false

    @NotBlank
    lateinit var inputTopic: String

    @NotBlank
    lateinit var outputTopic: String
    lateinit var health: KafkaHealthProperties
}

data class KafkaHealthProperties(
    val enabled: Boolean = false,
    @field:NotBlank
    val topic: String,
)

