package com.dreweaster.ddd.kestrel.infrastructure.http.eventstream.consumer.offset

import reactor.core.publisher.Mono

interface OffsetManager {

    fun getOffset(offsetKey: String): Mono<Long?>

    fun saveOffset(offsetKey: String, offset: Long): Mono<Unit>
}

object InMemoryOffsetManager : OffsetManager {

    private var offsetsMap: Map<String, Long> = emptyMap()

    override fun getOffset(offsetKey: String) = offsetsMap[offsetKey]?.let { Mono.just(it) } ?: Mono.empty()

    override fun saveOffset(offsetKey: String, offset: Long) = Mono.fromCallable { offsetsMap += offsetKey to offset }
}