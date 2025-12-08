package com.example.demokafka.properties

import jakarta.validation.Valid
import jakarta.validation.constraints.NotBlank
import org.hibernate.validator.constraints.time.DurationMax
import org.hibernate.validator.constraints.time.DurationMin
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.validation.annotation.Validated
import java.time.Duration

const val PREFIX = "demo"

@ConfigurationProperties(PREFIX, ignoreUnknownFields = false)
@Validated
data class DemoProperties(
    @field:Valid
    val kafka: DemoKafkaProperties,
)

data class DemoKafkaProperties(
    @field:Valid
    val consumeProduce: CustomKafkaProperties,
    @field:Valid
    val produceConsume: CustomKafkaProperties,
)

data class CustomKafkaProperties(
    val enabled: Boolean,

    @field:NotBlank
    val inputTopic: String,
    @field:NotBlank
    val outputTopic: String,

    @field:DurationMin(millis = 1000)
    @field:DurationMax(millis = 10_000)
    val consumerLag: Duration,

    @field:Valid
    val health: KafkaHealthProperties,
)

data class KafkaHealthProperties(
    val enabled: Boolean = false,
    @field:NotBlank
    val outputTopic: String,
)
