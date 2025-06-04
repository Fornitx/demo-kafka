package com.example.demokafka.properties

import jakarta.validation.Valid
import jakarta.validation.constraints.NotBlank
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
    @field:DurationMin(millis = 100)
    val produceConsumeTimeout: Duration,

    val produceConsumeNewTemplate: Boolean = false,
)

data class CustomKafkaProperties(
    val enabled: Boolean,

    @field:NotBlank
    val inputTopic: String,
    @field:NotBlank
    val outputTopic: String,

    @field:Valid
    val health: KafkaHealthProperties,
)

data class KafkaHealthProperties(
    val enabled: Boolean = false,
    @field:NotBlank
    val topic: String,
)
