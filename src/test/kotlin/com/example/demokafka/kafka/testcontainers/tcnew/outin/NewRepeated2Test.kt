package com.example.demokafka.kafka.testcontainers.tcnew.outin

import com.example.demokafka.TestProfiles
import com.example.demokafka.kafka.model.DemoRequest
import com.example.demokafka.kafka.model.DemoResponse
import com.example.demokafka.kafka.services.ProduceAndConsumeKafkaService
import com.example.demokafka.kafka.services.ProduceAndConsumeKafkaServiceNewImpl
import com.example.demokafka.kafka.testcontainers.tcnew.AbstractNewTestcontainersKafkaTest
import com.example.demokafka.properties.PREFIX
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.test.runTest
import org.apache.kafka.common.header.internals.RecordHeader
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.RepeatedTest
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.kafka.support.KafkaUtils
import org.springframework.test.context.ActiveProfiles
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

@SpringBootTest(
    properties = ["$PREFIX.kafka.out-in-new-template=true"]
)
@ActiveProfiles(TestProfiles.OUT_IN)
class NewRepeated2Test : AbstractNewTestcontainersKafkaTest() {
    @Autowired
    private lateinit var service: ProduceAndConsumeKafkaService

    @RepeatedTest(5)
    fun test() = runTest {
        assertThat(service).isInstanceOf(ProduceAndConsumeKafkaServiceNewImpl::class.java)

        // call service
        val request = DemoRequest("Abc")
        val deferred = async(Dispatchers.IO) { service.sendAndReceive(request) }

        // consume sent record
        val records = consume(properties.kafka.outIn.outputTopic)
        assertEquals(1, records.count())

        val sentRecord = records.first()
        log.info { "Sent record: ${KafkaUtils.format(sentRecord)}" }

        // check reply topic header
        val replyTopicHeader = sentRecord.headers().lastHeader(KafkaHeaders.REPLY_TOPIC)
        assertNotNull(replyTopicHeader)
        assertArrayEquals(
            properties.kafka.outIn.inputTopic.toByteArray(Charsets.UTF_8),
            replyTopicHeader.value()
        )

        // correlationId topic header
        val correlationIdHeader = sentRecord.headers().lastHeader(KafkaHeaders.CORRELATION_ID)
        assertNotNull(replyTopicHeader)

        val correlationId = correlationIdHeader.value()

        // check record value
        assertEquals(objectMapper.writeValueAsString(request), sentRecord.value())

        // send response
        val response = DemoResponse(request.msg.repeat(3))
        produce(
            properties.kafka.outIn.inputTopic,
            objectMapper.writeValueAsString(response),
            listOf(RecordHeader(KafkaHeaders.CORRELATION_ID, correlationId)),
        )

        // check service response
        val serviceResponse = deferred.await()
        assertEquals(response, serviceResponse)
    }
}
