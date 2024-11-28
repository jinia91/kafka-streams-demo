package kr.co.kafka.streams

import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.springframework.kafka.streams.KafkaStreamsInteractiveQueryService
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController


@RestController
class KafkaStateStoreController(
    private val kafkaStreamsInteractiveQueryService: KafkaStreamsInteractiveQueryService,
) {

    @GetMapping("/state-store")
    fun appStore(@RequestParam store: String): String {
        val appStore =
            kafkaStreamsInteractiveQueryService.retrieveQueryableStore(store, QueryableStoreTypes.windowStore<Any, Any>())
        return appStore.all().asSequence().joinToString("\n")
    }
}