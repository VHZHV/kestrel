package com.dreweaster.ddd.kestrel.application

import java.time.Duration
import java.time.Instant

interface CommandDeduplicationStrategy {
    fun isDuplicate(commandId: CommandId): Boolean
}

interface CommandDeduplicationStrategyBuilder {
    fun addEvent(domainEvent: PersistedEvent<*>): CommandDeduplicationStrategyBuilder

    fun build(): CommandDeduplicationStrategy
}

interface CommandDeduplicationStrategyFactory {
    fun newBuilder(): CommandDeduplicationStrategyBuilder
}

class TimeRestrictedCommandDeduplicationStrategy(private val causationIds: Set<CausationId>) : CommandDeduplicationStrategy {
    override fun isDuplicate(commandId: CommandId): Boolean = causationIds.contains(CausationId(commandId.value))

    class Builder(private val barrierDate: Instant) : CommandDeduplicationStrategyBuilder {
        private var causationIds: Set<CausationId> = emptySet()

        override fun addEvent(domainEvent: PersistedEvent<*>): CommandDeduplicationStrategyBuilder {
            if (domainEvent.timestamp.isAfter(barrierDate)) {
                causationIds += domainEvent.causationId
            }
            return this
        }

        override fun build(): CommandDeduplicationStrategy = TimeRestrictedCommandDeduplicationStrategy(causationIds)
    }
}

object TwentyFourHourWindowCommandDeduplication : CommandDeduplicationStrategyFactory {
    override fun newBuilder(): CommandDeduplicationStrategyBuilder =
        TimeRestrictedCommandDeduplicationStrategy.Builder(Instant.now().minus(Duration.ofHours(24)))
}
