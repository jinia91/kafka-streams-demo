package kr.co.kafka.consumer

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.data.jpa.repository.config.EnableJpaAuditing

@SpringBootApplication
@EnableJpaAuditing
class KafkaConsumerSpringApplication

fun main(args: Array<String>) {
    runApplication<KafkaConsumerSpringApplication>(*args)
}