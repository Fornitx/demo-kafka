package com.example.demokafka.utils

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.core.log.LogAccessor
import org.springframework.kafka.support.serializer.DeserializationException
import org.springframework.kafka.support.serializer.SerializationUtils

private val log = KotlinLogging.logger {}

object DemoKafkaUtils {
    fun checkForErrors(record: ConsumerRecord<*, *>): Exception? =
        if (record.value() == null || record.key() == null) checkDeserialization(record) else null

    fun checkDeserialization(record: ConsumerRecord<*, *>): DeserializationException? {
        val logAccessor = LogAccessor(DemoKafkaUtils::class.java)
        var exception = SerializationUtils.getExceptionFromHeader(
            record, SerializationUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER, logAccessor
        )
        if (exception != null) {
            log.error(exception) {
                "Reply value deserialization failed for ${record.topic()}-${record.partition()}@${record.offset()}"
            }
            return exception
        }
        exception = SerializationUtils.getExceptionFromHeader(
            record, SerializationUtils.KEY_DESERIALIZER_EXCEPTION_HEADER, logAccessor
        )
        if (exception != null) {
            log.error(exception) {
                "Reply key deserialization failed for ${record.topic()}-${record.partition()}@${record.offset()}"
            }
            return exception
        }
        return null
    }
}
