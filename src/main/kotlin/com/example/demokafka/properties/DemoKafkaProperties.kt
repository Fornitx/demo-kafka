package com.example.demokafka.properties

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.validation.annotation.Validated

const val PREFIX = "demo"

@ConfigurationProperties(PREFIX, ignoreUnknownFields = false)
@Validated
data class DemoKafkaProperties(
    val kafka: DemoKafkaKafkaProperties,
)
