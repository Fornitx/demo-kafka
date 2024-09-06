package com.example.demokafka

import com.example.demokafka.properties.DemoKafkaProperties
import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.beans.factory.annotation.Autowired
import kotlin.reflect.jvm.jvmName

abstract class AbstractTest {
    protected val log = KotlinLogging.logger(this::class.jvmName)

    @Autowired
    protected lateinit var properties: DemoKafkaProperties
}
