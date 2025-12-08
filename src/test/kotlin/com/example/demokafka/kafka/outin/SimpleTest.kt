package com.example.demokafka.kafka.outin

import com.example.demokafka.TestProfiles
import com.example.demokafka.kafka.AbstractTestcontainersKafkaTest
import com.example.demokafka.kafka.model.DemoRequest
import com.example.demokafka.kafka.model.DemoResponse
import com.example.demokafka.kafka.services.ProduceAndConsumeKafkaService
import com.example.demokafka.utils.log
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.test.runTest
import org.apache.kafka.common.header.internals.RecordHeader
import org.junit.jupiter.api.RepeatedTest
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.kafka.support.KafkaUtils
import org.springframework.test.context.ActiveProfiles
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

@SpringBootTest
@ActiveProfiles(TestProfiles.PRODUCE_CONSUME)
class SimpleTest : AbstractTestcontainersKafkaTest() {
    @Autowired
    private lateinit var service: ProduceAndConsumeKafkaService

    @RepeatedTest(2)
    fun test() = runTest {
        // call service
        val request = DemoRequest("Abc")
        val deferred = async(Dispatchers.IO) { service.sendAndReceive(request) }

        // consume sent record
        val sentRecord = consumeSingle(properties.kafka.produceConsume.outputTopic)
        log.info { sentRecord.log() }

        log.info { "Sent record: ${KafkaUtils.format(sentRecord)}" }

        // check reply topic header
        val replyTopicHeader = sentRecord.headers().lastHeader(KafkaHeaders.REPLY_TOPIC)
        // TODO reply partition
        assertNotNull(replyTopicHeader)
        assertEquals(properties.kafka.produceConsume.inputTopic, replyTopicHeader.value().decodeToString())

        // check reply partition header
        val replyPartitionHeader = sentRecord.headers().lastHeader(KafkaHeaders.REPLY_PARTITION)
        assertNotNull(replyPartitionHeader)

        // correlationId topic header
        val correlationIdHeader = sentRecord.headers().lastHeader(KafkaHeaders.CORRELATION_ID)
        assertNotNull(replyTopicHeader)

        val correlationId = correlationIdHeader.value()

        // check record value
        assertEquals(jsonMapper.writeValueAsString(request), sentRecord.value())

        // send response
        val response = DemoResponse(request.msg1.repeat(3))
        produce(
            properties.kafka.produceConsume.inputTopic,
            jsonMapper.writeValueAsString(response),
            listOf(RecordHeader(KafkaHeaders.CORRELATION_ID, correlationId)),
        )

        // check service response
        val serviceResponse = deferred.await()
        assertEquals(response, serviceResponse)
    }
}
