package com.example.demokafka

import com.example.demokafka.utils.DemoKafkaContainer
import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer
import org.springframework.boot.test.context.assertj.AssertableApplicationContext
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.boot.test.context.runner.ContextConsumer
import org.testcontainers.lifecycle.Startables
import kotlin.reflect.jvm.jvmName

abstract class AbstractProfileTest {
    protected val log = KotlinLogging.logger(this::class.jvmName)

    protected fun contextRunner(): ApplicationContextRunner = ApplicationContextRunner()
        .withInitializer(ConfigDataApplicationContextInitializer())
        .withUserConfiguration(DemoKafkaApplication::class.java)

    protected fun ApplicationContextRunner.withProfiles(vararg profiles: String): ApplicationContextRunner {
        require(profiles.isNotEmpty())
        return this.withPropertyValues("spring.profiles.active=${profiles.joinToString()}")
    }

    protected fun ApplicationContextRunner.runLogging(
        contextConsumer: ContextConsumer<AssertableApplicationContext>
    ) = this.run { context ->
        if (context.startupFailure != null) {
            log.error(context.startupFailure) {}
        }
        contextConsumer.accept(context)
    }

    companion object {
        protected val kafkaContainer = DemoKafkaContainer

        init {
            Startables.deepStart(kafkaContainer).join()
        }
    }
}
