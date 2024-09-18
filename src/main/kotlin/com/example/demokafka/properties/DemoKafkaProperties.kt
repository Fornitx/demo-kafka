package com.example.demokafka.properties

import org.springframework.boot.context.properties.ConfigurationProperties

const val PREFIX = "demo"

@ConfigurationProperties(PREFIX, ignoreUnknownFields = false)
data class DemoKafkaProperties(
    val kafka: DemoKafkaKafkaProperties,
)
