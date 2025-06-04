package com.example.demokafka.utils

import io.github.oshai.kotlinlogging.KotlinLogging
import org.testcontainers.utility.TestcontainersConfiguration

private val log = KotlinLogging.logger {}

object TestcontainersHelper {
    private val tcCfg: TestcontainersConfiguration = TestcontainersConfiguration.getInstance()

    val KAFKA_IMAGE: String = tcCfg.getEnvVarOrProperty("kafka.container.image", "")
}
