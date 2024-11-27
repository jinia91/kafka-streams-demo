package kr.co.kafka.consumer

import Temperature
import java.time.Instant
import java.time.ZoneId
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional

@Component
class KafkaSampleConsumer(
    private val temperatureRepository: TemperatureRepository,
) {
    @KafkaListener(
        topics = ["temperatureInC"],
        containerFactory = "kafkaListenerContainerFactory",
    )
    fun consume(record: ConsumerRecord<String, Temperature>, ack: Acknowledgment) {
        val temperature = record.value()
        val timestamp = record.timestamp()
        val eventCreatedAt = Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault()).toLocalDateTime()
        val entity = TemperatureEntity(
            temperature = temperature.temperature,
            unit = temperature.unit,
            location = temperature.sensorLocation,
            eventCreatedAt = eventCreatedAt,
        )
        temperatureRepository.save(entity)
    }

    @KafkaListener(
        topics = ["averageTemperatureInC"],
        containerFactory = "kafkaListenerContainerFactory",
    )
    fun consume2(record: ConsumerRecord<String, Temperature>, ack: Acknowledgment) {
        val averageTemperature = record.value()
        println("average temperature: ${averageTemperature.temperature}")
    }
}