package com.dreweaster.ddd.kestrel.infrastructure.http.eventstream.consumer.offset

interface OffsetManager {
    suspend fun getOffset(offsetKey: String): Long?

    suspend fun saveOffset(
        offsetKey: String,
        offset: Long,
    )
}

object InMemoryOffsetManager : OffsetManager {
    private var offsetsMap: Map<String, Long> = emptyMap()

    override suspend fun getOffset(offsetKey: String) = offsetsMap[offsetKey]

    override suspend fun saveOffset(
        offsetKey: String,
        offset: Long,
    ) {
        offsetsMap += offsetKey to offset
    }
}
