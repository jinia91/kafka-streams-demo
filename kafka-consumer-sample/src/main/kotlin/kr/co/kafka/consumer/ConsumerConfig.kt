package kr.co.kafka.consumer

import Temperature
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.listener.ContainerProperties


@Configuration
@EnableKafka
class ConsumerConfig(
) {
    @Bean
    fun consumerFactory() = DefaultKafkaConsumerFactory<String, Temperature>(
        consumerProps,
        StringDeserializer(),
        KafkaProtobufDeserializer()
        )

    private val consumerProps = mapOf(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:10001, localhost:10002, localhost:10000",
        ConsumerConfig.GROUP_ID_CONFIG to "group-1",
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false",
        ConsumerConfig.ISOLATION_LEVEL_CONFIG to "read_committed",
        "schema.registry.url" to "http://localhost:8081",
        "specific.protobuf.value.type" to Temperature::class.java,

        )

    @Bean
    fun kafkaListenerContainerFactory(consumerFactory: ConsumerFactory<String, Temperature>) =
        ConcurrentKafkaListenerContainerFactory<String, Temperature>()
            .also {
                it.consumerFactory = consumerFactory
                it.containerProperties.ackMode = ContainerProperties.AckMode.MANUAL
            }

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
