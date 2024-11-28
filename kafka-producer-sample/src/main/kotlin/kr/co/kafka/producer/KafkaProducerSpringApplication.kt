package kr.co.kafka.producer

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class KafkaProducerSpringApplication

fun main(args: Array<String>) {
    val context = runApplication<KafkaProducerSpringApplication>(*args)

    val producer = context.getBean(KafkaSampleProducer::class.java)

    val temperatureList = listOf(
        51,51,51,50,55,45,50,49,49,49,
        39,59,39,59,59,59,79,59,59,79,
        51,51,51,50,55,45,50,49,49,49,
        39,59,39,59,59,59,79,59,59,79,
    )

    while (true) {
//    temperatureList.forEach {
        producer.publish(Temperature.newBuilder()
            .setSensorLocation("Daegu")
//            .setTemperature(it.toDouble())
            .setTemperature(kotlin.random.Random.nextDouble(23.0, 30.2))
            .setUnit("F")
            .build())
        Thread.sleep(1000)
    }
}
