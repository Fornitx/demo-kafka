package com.example.demokafka.utils

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.header.internals.RecordHeaders
import org.springframework.core.log.LogAccessor
import org.springframework.kafka.support.KafkaUtils
import org.springframework.kafka.support.serializer.DeserializationException
import org.springframework.kafka.support.serializer.SerializationUtils
import java.io.Serializable
import java.nio.ByteBuffer

private val log = KotlinLogging.logger {}

object DemoKafkaUtils {
    fun checkForErrors(record: ConsumerRecord<*, *>): Exception? =
        if (record.value() == null || record.key() == null) checkDeserialization(record) else null

    private fun checkDeserialization(record: ConsumerRecord<*, *>): DeserializationException? {
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

//inline fun <reified T> typeRef() = object : TypeReference<T>() {}

fun ConsumerRecord<*, *>.log(): String = "ConsumerRecord received: ${KafkaUtils.format(this)}\n" +
        "\tkey     : ${this.key()}\n" +
        "\tvalue   : ${this.value()}\n" +
        "\theaders : ${this.headers()}"

fun ByteArray.decodeToInt(): Int = ByteBuffer.wrap(this).int

fun headers(vararg pairs: Pair<String, Serializable>): RecordHeaders? =
    if (pairs.isEmpty()) {
        null
    } else {
        RecordHeaders(pairs.map { (k, v) ->
            RecordHeader(
                k, when (v) {
                    is String -> v.encodeToByteArray()
                    is Int -> v.encodeToByteArray()
                    is ByteArray -> v
                    else -> throw IllegalArgumentException("$v is ${v::class}, which is not supported")
                }
            )
        })
    }

fun Int.encodeToByteArray(): ByteArray {
    return ByteBuffer.allocate(Int.SIZE_BYTES).putInt(this).array()
}
