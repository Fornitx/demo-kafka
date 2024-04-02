package com.example.demokafka

import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.boot.availability.ApplicationAvailability
import org.springframework.boot.availability.AvailabilityChangeEvent
import org.springframework.boot.context.event.SpringApplicationEvent
import org.springframework.context.ApplicationEvent
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component

private val log = KotlinLogging.logger { }

@Component
class DemoComponent(private val availability: ApplicationAvailability) {
    @EventListener
    fun event(event: SpringApplicationEvent) {
        innerEvent(event)
    }

    @EventListener
    fun event2(event: AvailabilityChangeEvent<*>) {
        innerEvent(event)
    }

    private fun innerEvent(event: ApplicationEvent) {
        log.info("{} {} {}", event::class.simpleName, availability.livenessState, availability.readinessState)
    }
}
