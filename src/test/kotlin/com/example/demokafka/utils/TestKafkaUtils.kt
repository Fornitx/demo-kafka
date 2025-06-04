package com.example.demokafka.utils

import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader
import java.io.Serializable
import java.nio.ByteBuffer

fun headers(vararg pairs: Pair<String, Serializable>): Iterable<Header>? =
    if (pairs.isEmpty()) {
        null
    } else {
        pairs.map { (k, v) ->
            val value = when (v) {
                is String -> v.encodeToByteArray()
                is Int -> v.encodeToByteArray()
                else -> throw IllegalArgumentException("$v is ${v::class}, which is not supported")
            }
            RecordHeader(k, value)
        }
    }

fun Int.encodeToByteArray(): ByteArray {
    return ByteBuffer.allocate(Int.SIZE_BYTES).putInt(this).array()
}
