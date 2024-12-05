package kr.co.kafka.consumer.streams

import Temperature
import jakarta.annotation.PostConstruct
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.rocksdb.RocksDB
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component
import kotlin.concurrent.timer

object StreamsMetaData {
    const val WINDOW_SIZE = 10_000
    val WINDOW_START_TIMESTAMP = System.currentTimeMillis()
}

@Component
class KafkaStreamingConsumer(
    private val rocksDB: RocksDB,
    private val kafkaTemplate: KafkaTemplate<String, Temperature>,
    ) {

    @PostConstruct
    fun init() {
        timer(daemon = true, period = StreamsMetaData.WINDOW_SIZE.toLong()) {
            val currentTime = System.currentTimeMillis()
            val windowStartTime = ((currentTime - StreamsMetaData.WINDOW_START_TIMESTAMP) / StreamsMetaData.WINDOW_SIZE  - 1)* StreamsMetaData.WINDOW_SIZE + StreamsMetaData.WINDOW_START_TIMESTAMP
            val windowEndTime = windowStartTime + StreamsMetaData.WINDOW_SIZE
            val key = "$windowStartTime:$windowEndTime"

            val aggregateStates : MutableMap<String, AvgTemperatureState> = mutableMapOf()
            rocksDB.newIterator().use {iterator ->
                iterator.seek(key.toByteArray())
                while (iterator.isValid) {
                    val (sum, count) = iterator.value().toString(Charsets.UTF_8).split(",").map { it.toDouble() }
                    aggregateStates[iterator.key().toString()] = AvgTemperatureState(sum, count.toInt())
                    iterator.next()
                }
            }

            println("produce==========$key , $aggregateStates")
            aggregateStates.forEach { (key, value) ->
                kafkaTemplate.send("avgTemperatureInC3", key, Temperature.newBuilder()
                    .setSensorLocation("Daegu")
                    .setTemperature(value.sum / value.count)
                    .setUnit("C")
                    .build())
            }
        }
    }

    data class AvgTemperatureState(
        val sum: Double,
        val count: Int,
    )

    @KafkaListener(
        topics = ["temperatureInC"],
        containerFactory = "kafkaListenerContainerFactory",
    )
    fun consume(record: ConsumerRecord<String, Temperature>, ack: Acknowledgment) {
        val initialWindowTime = StreamsMetaData.WINDOW_START_TIMESTAMP
        val windowStartTime = (record.timestamp() - initialWindowTime) / StreamsMetaData.WINDOW_SIZE * StreamsMetaData.WINDOW_SIZE + initialWindowTime
        val windowEndTime = windowStartTime + StreamsMetaData.WINDOW_SIZE
        val key = "$windowStartTime:$windowEndTime"


        if (windowEndTime < System.currentTimeMillis()) {
            ack.acknowledge()
            return
        }

        val aggregateState : AvgTemperatureState = try {
            rocksDB.get(key.toByteArray()).toAggregateState()
        } catch (e: Exception) {
            AvgTemperatureState(0.0, 0)
        }

        println("consume==========$key, $aggregateState")

        val newSum = aggregateState.sum + record.value().temperature
        val newCount = aggregateState.count + 1
        rocksDB.put(key.toByteArray(), "$newSum,$newCount".toByteArray())
        ack.acknowledge()
    }

    private fun ByteArray.toAggregateState(): AvgTemperatureState {
        val (sum, count) = this.toString(Charsets.UTF_8).split(",")
        return AvgTemperatureState(sum.toDouble(), count.toInt())
    }

}