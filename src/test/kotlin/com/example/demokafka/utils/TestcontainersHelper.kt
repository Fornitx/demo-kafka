package com.example.demokafka.utils

import org.testcontainers.utility.TestcontainersConfiguration

object TestcontainersHelper {
    private val tcCfg: TestcontainersConfiguration = TestcontainersConfiguration.getInstance()

    val KAFKA_IMAGE: String = tcCfg.getEnvVarOrProperty("kafka.container.image", "")
}
