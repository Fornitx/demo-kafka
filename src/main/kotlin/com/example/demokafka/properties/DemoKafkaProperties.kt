package com.example.demokafka.properties

import jakarta.validation.constraints.NotBlank
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.boot.context.properties.ConfigurationProperties

const val PREFIX = "demo"

@ConfigurationProperties(PREFIX, ignoreUnknownFields = false)
data class DemoKafkaProperties(
    val inOutKafka: CustomKafkaProperties,
    val outInKafka: CustomKafkaProperties,
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
