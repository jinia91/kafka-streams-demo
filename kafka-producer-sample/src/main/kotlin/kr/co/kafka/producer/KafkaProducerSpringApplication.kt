package kr.co.kafka.producer

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class KafkaProducerSpringApplication

fun main(args: Array<String>) {
    val context = runApplication<KafkaProducerSpringApplication>(*args)

    val producer = context.getBean(KafkaSampleProducer::class.java)
    while (true) {
        producer.publish(Temperature.newBuilder()
            .setSensorLocation("Seoul")
            .setTemperature(kotlin.random.Random.nextDouble(23.0, 30.2))
            .setUnit("F")
            .build())
        Thread.sleep(1000)
    }
}
