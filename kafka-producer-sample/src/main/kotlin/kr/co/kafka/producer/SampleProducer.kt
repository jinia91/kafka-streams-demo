package kr.co.kafka.producer

import Temperature
import org.apache.kafka.clients.producer.ProducerRecord
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component


@Component
class KafkaSampleProducer(
    private val kafkaTemplate: KafkaTemplate<String, Temperature>,
) {
    fun publish(
        temperature: Temperature
    ) {
        val producerRecord = ProducerRecord(
            "temperature",
            temperature.sensorLocation,
            temperature
        )

        kafkaTemplate.send(producerRecord)
    }
}
