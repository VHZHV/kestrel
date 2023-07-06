package com.dreweaster.ddd.kestrel.infrastructure

import com.dreweaster.ddd.kestrel.application.*
import com.dreweaster.ddd.kestrel.application.pagination.Page
import com.dreweaster.ddd.kestrel.application.pagination.Pageable
import com.dreweaster.ddd.kestrel.domain.Aggregate
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import com.dreweaster.ddd.kestrel.domain.DomainEventTag
import com.dreweaster.ddd.kestrel.domain.ProcessManager
import io.vavr.control.Try
import java.time.Instant
import java.util.UUID
import kotlin.reflect.KClass


interface InMemoryEventStreamHandlers {
    suspend fun loadEventStream(
        tags: Set<DomainEventTag>,
        afterOffset: Long,
        batchSize: Int,
    ): EventStream

    suspend fun loadEventStream(
        tags: Set<DomainEventTag>,
        afterInstant: Instant,
        batchSize: Int,
    ): EventStream
}

object UnsupportedOperationInMemoryEventStreamHandler : InMemoryEventStreamHandlers {
    override suspend fun loadEventStream(
        tags: Set<DomainEventTag>,
        afterOffset: Long,
        batchSize: Int
    ): EventStream {
        throw UnsupportedOperationException()
    }

    override suspend fun  loadEventStream(
        tags: Set<DomainEventTag>,
        afterInstant: Instant,
        batchSize: Int
    ): EventStream {
        throw UnsupportedOperationException()
    }
}

class SerialiseInMemoryEventStreamHandlers(
    private val mapping: EventPayloadMapper,
    private val inMemoryBackend: InMemoryBackend
) : InMemoryEventStreamHandlers {

    private fun retrieveEvents(
        tags: Set<DomainEventTag>,
        batchSize: Int,
        filter: (Pair<PersistedEvent<*>, Long>) -> Boolean
    ):EventStream{
        val matching = inMemoryBackend.events.filter {
            tags.contains(it.first.rawEvent.tag)
        }
        val maxOffset = matching.lastOrNull()?.second?:1L

        val window = matching.filter(filter)
            .take(batchSize)
            .map { (event, offset) ->
                val mapped = mapping.serialiseEvent(event.rawEvent)
                StreamEvent(
                    offset,
                    event.id,
                    event.aggregateType.blueprint.name,
                    event.aggregateId,
                    event.causationId,
                    event.correlationId,
                    event.eventType.qualifiedName!!,
                    event.rawEvent.tag,
                    event.timestamp,
                    event.sequenceNumber,
                    mapped.payload,
                    mapped.contentType
                )
            }

        return EventStream(
            events = window,
            tags = tags,
            batchSize = batchSize,
            startOffset = window.firstOrNull()?.offset,
            endOffset = window.lastOrNull()?.offset,
            maxOffset = maxOffset
        )
    }

    override suspend fun loadEventStream(
        tags: Set<DomainEventTag>,
        afterOffset: Long,
        batchSize: Int
    ):EventStream = retrieveEvents(tags,batchSize){
            it.second > afterOffset
    }

    override suspend fun loadEventStream(
        tags: Set<DomainEventTag>,
        afterInstant: Instant,
        batchSize: Int
    ): EventStream = retrieveEvents(tags,batchSize){
        it.first.timestamp > afterInstant
    }

}

open class InMemoryBackend(val streamers: InMemoryEventStreamHandlers = UnsupportedOperationInMemoryEventStreamHandler) :
    Backend {

    private var nextOffset: Long = 0L

    var events: List<Pair<PersistedEvent<*>, Long>> = emptyList()
        private set

    fun clear() {
        events = emptyList()
    }

    override suspend fun <E : DomainEvent, A : Aggregate<*, E, *>> persistAggregate(
        aggregateType: A,
        aggregateId: AggregateId,
        commandHandler: suspend (PersistedAggregate<E, A>) -> GeneratedEvents<E>,
    ): Try<List<PersistedEvent<E>>> {
        TODO("not implemented")
    }

    override suspend fun <E : DomainEvent, A : Aggregate<*, E, *>> loadEvents(
        aggregateType: A,
        aggregateId: AggregateId,
    ): List<PersistedEvent<E>> {
        return persistedEventsFor(aggregateType, aggregateId)
    }

    override suspend fun <E : DomainEvent, A : Aggregate<*, E, *>> loadEvents(
        aggregateType: A,
        aggregateId: AggregateId,
        afterSequenceNumber: Long,
    ): List<PersistedEvent<E>> {
        return persistedEventsFor(aggregateType, aggregateId).filter { it.sequenceNumber > afterSequenceNumber }
    }

    override suspend fun <E : DomainEvent, A : Aggregate<*, E, *>> saveEvents(
        aggregateType: A,
        aggregateId: AggregateId,
        causationId: CausationId,
        rawEvents: List<E>,
        expectedSequenceNumber: Long,
        correlationId: CorrelationId?,
    ): List<PersistedEvent<E>> {
        if (aggregateHasBeenModified(aggregateType, aggregateId, expectedSequenceNumber)) {
            throw OptimisticConcurrencyException
        }

        @Suppress("UNCHECKED_CAST")
        val persistedEvents =
            rawEvents.fold(Pair<Long, List<PersistedEvent<E>>>(expectedSequenceNumber + 1, emptyList())) { acc, e ->
                Pair(
                    acc.first + 1,
                    acc.second +
                            PersistedEvent(
                                EventId(UUID.randomUUID().toString()),
                                aggregateType,
                                aggregateId,
                                causationId,
                                correlationId,
                                e::class as KClass<E>,
                                1,
                                e,
                                Instant.now(),
                                acc.first,
                            ),
                )
            }.second

        persistedEvents.forEach { event ->
            events += Pair(event, nextOffset)
            nextOffset += 1
        }

        return persistedEvents
    }

    override suspend fun <E : DomainEvent, P : ProcessManager<*, E, *>> persistProcessManagerEvent(
        eventId: EventId,
        rawEvent: E,
        processManagerType: P,
        processManagerCorrelationId: ProcessManagerCorrelationId,
        causationId: CausationId,
    ) {
        TODO("not implemented")
    }

    override suspend fun findIdsForProcessManagersAwaitingProcessing(pageable: Pageable): Page<ProcessManagerCorrelationId> {
        TODO("not implemented")
    }

    override suspend fun <E : DomainEvent, P : ProcessManager<*, E, *>> executeProcessManager(
        type: P,
        id: ProcessManagerCorrelationId,
        force: Boolean,
        retryStrategy: ProcessManagerRetryStrategy,
        processHandler: suspend (PersistedProcessManager<E, P>) -> ProcessManagerProcessingResult,
    ): ProcessManagerProcessingResult {
        TODO("not implemented")
    }

    override suspend fun <E : DomainEvent> loadEventStream(
        tags: Set<DomainEventTag>,
        afterOffset: Long,
        batchSize: Int,
    ): EventStream = streamers.loadEventStream(tags, afterOffset, batchSize)

    override suspend fun <E : DomainEvent> loadEventStream(
        tags: Set<DomainEventTag>,
        afterInstant: Instant,
        batchSize: Int,
    ): EventStream = streamers.loadEventStream(tags, afterInstant, batchSize)

    @Suppress("UNCHECKED_CAST")
    private fun <E : DomainEvent> persistedEventsFor(
        aggregateType: Aggregate<*, E, *>,
        aggregateId: AggregateId,
    ): List<PersistedEvent<E>> {
        return events.filter { e ->
            val event = e as Pair<PersistedEvent<E>, Long>
            event.first.aggregateType == aggregateType && event.first.aggregateId == aggregateId
        }.map { event -> event.first as PersistedEvent<E> }
    }

    private fun <E : DomainEvent> aggregateHasBeenModified(
        aggregateType: Aggregate<*, E, *>,
        aggregateId: AggregateId,
        expectedSequenceNumber: Long?,
    ): Boolean {
        return persistedEventsFor(aggregateType, aggregateId)
            .lastOrNull()?.sequenceNumber?.equals(expectedSequenceNumber)?.not() == true
    }


}
