package kr.co.kafka.consumer

import java.util.concurrent.Executors
import org.springframework.transaction.annotation.Transactional
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter

@RestController
@RequestMapping("temperature")
class TemperatureSseController(
    private val temperatureRepository: TemperatureRepository,
) {
    private val executors = Executors.newCachedThreadPool()

    @GetMapping("/current")
    fun temperatureStream(): SseEmitter {
        val emitter = SseEmitter(0L)

        executors.submit {
            while (true) {
                val latest = temperatureRepository.findLatest()
                latest.forEach {
                    emitter.send(it)
                }
                Thread.sleep(5000)
            }
        }

        return emitter
    }
}