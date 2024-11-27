package kr.co.kafka.consumer

import jakarta.persistence.Entity
import jakarta.persistence.GeneratedValue
import jakarta.persistence.Id
import java.time.LocalDateTime

@Entity
class TemperatureEntity(
    @Id
    @GeneratedValue
    val id: Long? = null,
    val temperature: Double,
    val unit: String,
    val location: String,
    val eventCreatedAt: LocalDateTime,
    val processedAt: LocalDateTime = LocalDateTime.now(),
)