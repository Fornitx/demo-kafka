package com.example.demokafka.utils

import org.mockito.kotlin.any
import org.mockito.kotlin.atMost
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyNoMoreInteractions
import org.springframework.kafka.core.KafkaTemplate

fun KafkaTemplate<*, *>.verifyNoMoreInteractions() {
    verify(this, atMost(1)).afterSingletonsInstantiated()
    verify(this, atMost(1)).onApplicationEvent(any())
    verifyNoMoreInteractions(this)
}
