package kr.co.kafka.streams

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class KafkaStreamsSpringApplication

fun main(args: Array<String>) {
    runApplication<KafkaStreamsSpringApplication>(*args)
}

