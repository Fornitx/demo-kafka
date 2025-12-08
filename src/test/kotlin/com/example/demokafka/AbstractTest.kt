package com.example.demokafka

import com.example.demokafka.properties.DemoProperties
import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.beans.factory.annotation.Autowired
import tools.jackson.databind.json.JsonMapper
import kotlin.reflect.jvm.jvmName

abstract class AbstractTest {
    protected val log = KotlinLogging.logger(this::class.jvmName)

    @Autowired
    protected lateinit var jsonMapper: JsonMapper

    @Autowired
    protected lateinit var properties: DemoProperties
}
