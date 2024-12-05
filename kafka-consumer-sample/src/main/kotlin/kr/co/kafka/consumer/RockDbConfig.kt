package kr.co.kafka.consumer

import org.rocksdb.RocksDB
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class RockDbConfig{
    @Bean
    fun rocksDB(): RocksDB {
        RocksDB.loadLibrary()
        return RocksDB.open("/tmp/rocksdb")
    }
}