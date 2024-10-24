package com.example.demokafka

import com.example.demokafka.properties.DemoKafkaProperties
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.runApplication

@SpringBootApplication
@EnableConfigurationProperties(KafkaProperties::class, DemoKafkaProperties::class)
class DemoKafkaApplication

fun main(args: Array<String>) {
    runApplication<DemoKafkaApplication>(*args)
}
