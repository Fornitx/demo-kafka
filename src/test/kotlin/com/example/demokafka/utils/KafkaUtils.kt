package com.example.demokafka.utils

import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader

fun headers(vararg pairs: Pair<String, String>): Iterable<Header>? =
    if (pairs.isEmpty()) {
        null
    } else {
        pairs.map { RecordHeader(it.first, it.second.toByteArray(Charsets.UTF_8)) }
    }
