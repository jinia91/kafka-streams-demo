package kr.co.kafka.streams

import Temperature
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde
import java.time.Duration
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Branched
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Named
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.kstream.SlidingWindows
import org.apache.kafka.streams.kstream.TimeWindows
import org.apache.kafka.streams.kstream.ValueMapper
import org.apache.kafka.streams.kstream.Windows
import org.apache.kafka.streams.state.WindowStore
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafkaStreams
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration
import org.springframework.kafka.config.KafkaStreamsConfiguration
import org.springframework.kafka.config.StreamsBuilderFactoryBean
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer
import org.springframework.kafka.streams.KafkaStreamsInteractiveQueryService


@Configuration
@EnableKafkaStreams
class KafkaStreamsConfig {

    @Bean(name = [KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME])
    fun temperatureConfigs(): KafkaStreamsConfiguration {
        val props: MutableMap<String, Any> = HashMap()
        props[StreamsConfig.APPLICATION_ID_CONFIG] = "fToCStreams"
        props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:10001, localhost:10002, localhost:10000"
        props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String()::class.java
        props["schema.registry.url"] = "http://localhost:8081"
        props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = temperatureProtobufSerde()::class.java
        props["specific.protobuf.value.type"] = Temperature::class.java
        return KafkaStreamsConfiguration(props)
    }

    private fun temperatureProtobufSerde(): KafkaProtobufSerde<Temperature> {
        val protobufSerde: KafkaProtobufSerde<Temperature> = KafkaProtobufSerde<Temperature>()
        val serdeConfig: MutableMap<String, String> = HashMap()
        serdeConfig[SCHEMA_REGISTRY_URL_CONFIG] = "http://localhost:8081"
        protobufSerde.configure(serdeConfig, false)
        return protobufSerde
    }

    @Bean
    fun kafkaStreamsInteractiveQueryService(streamsBuilderFactoryBean: StreamsBuilderFactoryBean): KafkaStreamsInteractiveQueryService {
        val kafkaStreamsInteractiveQueryService =
            KafkaStreamsInteractiveQueryService(streamsBuilderFactoryBean)
        return kafkaStreamsInteractiveQueryService
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
    fun fToCStream(kStreamBuilder: StreamsBuilder): KStream<String, Temperature> {
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

    @Bean
    fun cProcessStream(kStreamBuilder: StreamsBuilder): KStream<String, String> {
        val originStream = kStreamBuilder.stream<String, Temperature>("temperatureInC")

        val branches = originStream.split(Named.`as`("celsius-branch-"))
            .branch({ _, value -> value.temperature < 0.0 }, Branched.`as`("negative"))
            .branch({ _, value -> value.temperature >= 0.0 }, Branched.`as`("positive"))
            .defaultBranch()

        val negativeStream = branches["celsius-branch-negative"]!!.mapValues {
            _, value -> "Temperature is below 0C: ${value.temperature}C"
        }

        val positiveStream = branches["celsius-branch-positive"]!!.mapValues {
            _, value -> "Temperature is above 0C: ${value.temperature}C"
        }

        val sink = negativeStream.merge(positiveStream)

        sink.to("temperatureInCProcess", Produced.with(Serdes.String(), Serdes.String()))
        return sink
    }

    @Bean
    fun averageInCStream(kStreamBuilder: StreamsBuilder): KStream<String, String> {
        val originStream = kStreamBuilder.stream<String, Temperature>("temperatureInC")

        data class SumAndCount (val sum: Double = 0.0, val count: Long = 0)

        val stream = originStream
            .mapValues<String>(ValueMapper<Temperature, String> { value: Temperature ->
                value.temperature.toString()
            })
            .groupByKey()
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(10)))
            .aggregate(
                {"0.0,0"},
                { _: String, value: String, aggregate: String ->
                    val (sum, count) = aggregate.split(",")
                    val newSum = sum.toDouble() + value.toDouble()
                    val newCount = count.toLong() + 1
                    "$newSum,$newCount"
                },
                Materialized.`as`<String?, String?, WindowStore<Bytes, ByteArray>?>("state-store2")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())

            )
            .toStream()
            .map { key, value ->
                val (sum, count) = value.split(",")
                KeyValue.pair(key.key(), (sum.toDouble() / count.toLong()).toString())
            }
        stream.to("averageTemperatureInC", Produced.with(Serdes.String(), Serdes.String()))
        return stream
    }

    fun fToC(f: Double): Double {
        return (f - 32) * 5.0 / 9.0
    }
}