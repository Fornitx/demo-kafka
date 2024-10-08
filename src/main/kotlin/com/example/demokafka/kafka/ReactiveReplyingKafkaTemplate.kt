package com.example.demokafka.kafka

import com.example.demokafka.kafka.metrics.DemoKafkaMetrics
import com.example.demokafka.utils.DemoKafkaUtils
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.mono
import kotlinx.coroutines.time.withTimeout
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.springframework.beans.factory.DisposableBean
import org.springframework.context.SmartLifecycle
import org.springframework.kafka.KafkaException
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate
import org.springframework.kafka.requestreply.KafkaReplyTimeoutException
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.kafka.support.KafkaUtils
import org.springframework.util.Assert
import reactor.core.Disposable
import reactor.core.publisher.Mono
import java.time.Duration
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import kotlin.concurrent.Volatile
import kotlin.random.Random

private val log = KotlinLogging.logger {}

@OptIn(ExperimentalStdlibApi::class)
class ReactiveReplyingKafkaTemplate<K, V, R>(
    private val producer: ReactiveKafkaProducerTemplate<K, V>,
    private val consumer: ReactiveKafkaConsumerTemplate<K, R>,
    private val metrics: DemoKafkaMetrics,
    private val defaultReplyTimeout: Duration,
    replyTopic: String,
) : SmartLifecycle, DisposableBean {
    private val replyTopic = replyTopic.toByteArray(Charsets.UTF_8)

    @Volatile
    private var running = false

    private val channels: ConcurrentHashMap<ByteArrayWrapper, SendChannel<ConsumerRecord<K, R>>> = ConcurrentHashMap()

    private var subscription: Disposable? = null

    suspend fun sendAndReceive(
        producerRecord: ProducerRecord<K, V>,
        replyTimeout: Duration = this.defaultReplyTimeout
    ): ConsumerRecord<K, R> {
        Assert.state(this.running, "Template has not been started")

        val correlationId = calcCorrelationId(producerRecord)

        val headers = producerRecord.headers()
        val hasReplyTopic = headers.lastHeader(KafkaHeaders.REPLY_TOPIC) != null
        if (!hasReplyTopic) {
            headers.add(RecordHeader(KafkaHeaders.REPLY_TOPIC, this.replyTopic))
        }
        headers.add(RecordHeader(KafkaHeaders.CORRELATION_ID, correlationId.bytes))

        log.info { "Sending: ${KafkaUtils.format(producerRecord)} with correlationId: $correlationId" }

        val channel = Channel<ConsumerRecord<K, R>>(1)
        this.channels[correlationId] = channel

        return try {
            withTimeout(replyTimeout) {
                try {
                    val senderResult = producer.send(producerRecord).awaitSingle()
                    metrics.kafkaProduce(producerRecord.topic()).increment()
                    log.info { "Sent: ${KafkaUtils.format(producerRecord)} with correlationId: $correlationId" }
                } catch (ex: Exception) {
                    metrics.kafkaProduceErrors(producerRecord.topic()).increment()
                    this@ReactiveReplyingKafkaTemplate.channels.remove(correlationId)
                    throw KafkaException("Send failed", ex)
                }

                channel.receive()
            }
        } catch (ex: TimeoutCancellationException) {
            log.warn { "Reply timed out for: ${KafkaUtils.format(producerRecord)} with correlationId: $correlationId" }
            throw KafkaReplyTimeoutException("Reply timed out")
        }
    }

    @Synchronized
    override fun start() {
        if (!this.running) {
            this.subscription = this.consumer
                .receiveAutoAck()
                .concatMap { consumerRecord ->
                    val correlationIdHeader = consumerRecord.headers().lastHeader(KafkaHeaders.CORRELATION_ID)
                    if (correlationIdHeader == null) {
                        log.warn { "correlationIdHeader is null" }
                    }
                    val correlationId = correlationIdHeader.value()
                    if (correlationId == null) {
                        log.warn { "correlationId is null" }
                    } else {
                        val channel = this.channels.remove(ByteArrayWrapper(correlationId))
                        if (channel == null) {
                            log.warn { "channel is null" }
                        } else {
                            val exception = DemoKafkaUtils.checkForErrors(consumerRecord)
                            if (exception != null) {
                                log.warn(exception) { "Deserialization error" }
                                channel.close(exception)
                            } else {
                                return@concatMap mono { channel.send(consumerRecord) }
                            }
                        }
                    }
                    return@concatMap Mono.empty()
                }
                .subscribe()
            this.running = true
        }
    }

    @Synchronized
    override fun stop() {
        if (this.running) {
            this.running = false
            subscription?.dispose()
            this.channels.clear()
        }
    }

    override fun isRunning(): Boolean = this.running

    override fun destroy() {
        producer.destroy()
    }

    private fun calcCorrelationId(record: ProducerRecord<K, V>): ByteArrayWrapper = ByteArrayWrapper()

    class ByteArrayWrapper(val bytes: ByteArray = Random.nextBytes(16)) {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as ByteArrayWrapper

            return bytes.contentEquals(other.bytes)
        }

        override fun hashCode(): Int = bytes.contentHashCode()

        override fun toString(): String = bytes.toHexString()
    }
}
