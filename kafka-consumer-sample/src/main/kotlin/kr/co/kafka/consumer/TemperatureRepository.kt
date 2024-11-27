package kr.co.kafka.consumer

import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Query

interface TemperatureRepository : JpaRepository<TemperatureEntity, Long>  {
    @Query("SELECT * FROM temperature_entity ORDER BY id DESC LIMIT 3", nativeQuery = true)
    fun findLatest(): List<TemperatureEntity>
}