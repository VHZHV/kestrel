package com.dreweaster.ddd.kestrel.application

import com.dreweaster.ddd.kestrel.domain.DomainEvent
import reactor.core.publisher.Mono
import kotlin.reflect.KClass

interface BoundedContextName { val name: String }

class BoundedContextEventSources(sources: List<Pair<BoundedContextName, BoundedContextEventSource>>) {

    private val sourcesMap = sources.toMap()

    operator fun get(name: BoundedContextName) = sourcesMap[name]
}

interface BoundedContextEventSource {

    class EventHandlersBuilder {

        var handlers: Map<KClass<out DomainEvent>, (DomainEvent, EventMetadata) -> Mono<Unit>> = emptyMap()

        fun <E: DomainEvent> withHandler(type: KClass<E>, handler: (E, EventMetadata) -> Mono<Unit>): EventHandlersBuilder {
            handlers += type to handler as (DomainEvent, EventMetadata) -> Mono<Unit>
            return this
        }

        fun build() = handlers
    }

    fun subscribe(handlers: Map<KClass<out DomainEvent>, (DomainEvent, EventMetadata) -> Mono<Unit>>, subscriberConfiguration: BoundedContextSubscriberConfiguration)
}

data class BoundedContextSubscriberConfiguration(val name: String, val edenPolicy: BoundedContextSubscriptionEdenPolicy)

data class EventMetadata(
        val eventId: EventId,
        val aggregateId: AggregateId,
        val causationId: CausationId,
        val correlationId: CorrelationId?,
        val sequenceNumber: Long)

enum class BoundedContextSubscriptionEdenPolicy { BEGINNING_OF_TIME, FROM_NOW }