package kr.co.kafka.streams

import Temperature
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.ValueMapper
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafkaStreams
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration
import org.springframework.kafka.config.KafkaStreamsConfiguration
import org.springframework.kafka.config.StreamsBuilderFactoryBean
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer

@Configuration
@EnableKafkaStreams
class KafkaStreamsConfig {
    @Bean(name = [KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME])
    fun kStreamsConfigs(): KafkaStreamsConfiguration {
        val props: MutableMap<String, Any> = HashMap()
        props[StreamsConfig.APPLICATION_ID_CONFIG] = "testStreams"
        props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:10001, localhost:10002, localhost:10000"
        props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String()::class.java
        props["schema.registry.url"] = "http://localhost:8081"
        props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = testProtobufSerde()::class.java
        props["specific.protobuf.value.type"] = Temperature::class.java
        return KafkaStreamsConfiguration(props)
    }

    private fun testProtobufSerde(): KafkaProtobufSerde<Temperature> {
        val protobufSerde: KafkaProtobufSerde<Temperature> = KafkaProtobufSerde<Temperature>()
        val serdeConfig: MutableMap<String, String> = HashMap()
        serdeConfig[SCHEMA_REGISTRY_URL_CONFIG] = "http://localhost:8081"
        protobufSerde.configure(serdeConfig, false)
        return protobufSerde
    }

    @Bean
    fun configurer(): StreamsBuilderFactoryBeanConfigurer {
        return StreamsBuilderFactoryBeanConfigurer { fb: StreamsBuilderFactoryBean ->
            fb.setStateListener { newState: KafkaStreams.State, oldState: KafkaStreams.State ->
                println("State transition from $oldState to $newState")
            }
        }
    }

    @Bean
    fun kStream(kStreamBuilder: StreamsBuilder): KStream<String, Temperature> {
        val stream = kStreamBuilder.stream<String, Temperature>("temperature")
        stream
            .mapValues<Temperature>(ValueMapper<Temperature, Temperature> { value: Temperature ->
                val celsius = fToC(value.temperature).let { String.format("%.2f", it).toDouble() }
                Temperature.newBuilder()
                    .setSensorLocation(value.sensorLocation)
                    .setTemperature(celsius)
                    .setUnit("C")
                    .build()
            })
            .to("temperatureInC")
        return stream
    }

    fun fToC(f: Double): Double {
        return (f - 32) * 5.0 / 9.0
    }
}