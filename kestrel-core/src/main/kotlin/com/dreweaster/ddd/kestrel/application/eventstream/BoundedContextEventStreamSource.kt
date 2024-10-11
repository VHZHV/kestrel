package com.dreweaster.ddd.kestrel.application.eventstream

import com.dreweaster.ddd.kestrel.application.AggregateId
import com.dreweaster.ddd.kestrel.application.CausationId
import com.dreweaster.ddd.kestrel.application.CorrelationId
import com.dreweaster.ddd.kestrel.application.EventId
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import kotlin.reflect.KClass

interface BoundedContextName {
    val name: String
}

class BoundedContextEventStreamSources(sources: List<Pair<BoundedContextName, BoundedContextEventStreamSource>>) {
    private val sourcesMap = sources.toMap()

    operator fun get(name: BoundedContextName) = sourcesMap[name]
}

interface BoundedContextEventStreamSource {
    class EventHandlersBuilder {
        private var handlers: Map<KClass<out DomainEvent>, (suspend (DomainEvent, EventMetadata) -> Unit)> = emptyMap()

        fun <E : DomainEvent> withHandler(type: KClass<E>, handler: suspend (E, EventMetadata) -> Unit): EventHandlersBuilder {
            @Suppress("UNCHECKED_CAST")
            handlers += type to handler as suspend (DomainEvent, EventMetadata) -> Unit
            return this
        }

        fun build() = handlers
    }

    fun subscribe(
        handlers: Map<KClass<out DomainEvent>, (suspend (DomainEvent, EventMetadata) -> Unit)>,
        subscriberConfiguration: EventStreamSubscriberConfiguration,
    )
}

data class EventStreamSubscriberConfiguration(val name: String, val edenPolicy: EventStreamSubscriptionEdenPolicy)

data class EventMetadata(
    val eventId: EventId,
    val aggregateId: AggregateId,
    val causationId: CausationId,
    val correlationId: CorrelationId?,
    val sequenceNumber: Long,
)

enum class EventStreamSubscriptionEdenPolicy { BEGINNING_OF_TIME, FROM_NOW }
