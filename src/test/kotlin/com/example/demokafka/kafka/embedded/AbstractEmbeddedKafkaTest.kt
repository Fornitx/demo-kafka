package com.example.demokafka.kafka.embedded

import com.example.demokafka.TestProfiles
import com.example.demokafka.kafka.AbstractKafkaTest
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.kafka.test.EmbeddedKafkaBroker
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.context.ActiveProfiles

@EmbeddedKafka(kraft = true)
@ActiveProfiles(TestProfiles.EMBEDDED)
abstract class AbstractEmbeddedKafkaTest : AbstractKafkaTest() {
    override val bootstrapServers: String
        get() = embeddedKafkaBroker.brokersAsString

    @Autowired
    protected lateinit var embeddedKafkaBroker: EmbeddedKafkaBroker
}
