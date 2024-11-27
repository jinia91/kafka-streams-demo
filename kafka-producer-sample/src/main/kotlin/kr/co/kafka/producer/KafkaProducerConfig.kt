package kr.co.kafka.producer

import Temperature
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate

@Configuration
class KafkaProducerConfig {
    @Bean
    fun defaultKafkaProducerFactory() =
        DefaultKafkaProducerFactory<String, Temperature>(
            kafkaProducerProperties(),
            StringSerializer(),
            KafkaProtobufSerializer()
        )

    fun kafkaProducerProperties() = mapOf(
        "bootstrap.servers" to "localhost:10001, localhost:10002, localhost:10000",
        "schema.registry.url" to "http://localhost:8081",
    )

    @Bean
    fun kafkaTemplate() = KafkaTemplate(defaultKafkaProducerFactory())
}