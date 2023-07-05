package com.dreweaster.ddd.kestrel.application.reporting.opentelemetry.traces

import com.dreweaster.ddd.kestrel.application.*
import com.dreweaster.ddd.kestrel.domain.Aggregate
import com.dreweaster.ddd.kestrel.domain.AggregateState
import com.dreweaster.ddd.kestrel.domain.DomainCommand
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.SpanKind
import io.opentelemetry.api.trace.Tracer

const val aggregateTypeKey = "aggregate_type"
const val commandTypeKey = "command_type"

class OpenTelemetryTraceDomainModelReporter(openTelemetry: OpenTelemetry) : DomainModelReporter {

    private val tracer: Tracer = openTelemetry.getTracer("com.dreweaster.ddd.kestrel.DomainModelReporter")

    val commandExecutionLatency = tracer.spanBuilder("aggregate_command_execution_latency_seconds")
        .setSpanKind(SpanKind.INTERNAL)

    val aggregateRecoveryLatency = tracer.spanBuilder("aggregate_recovery_latency_seconds")
        .setSpanKind(SpanKind.INTERNAL)

    val applyCommandLatency = tracer.spanBuilder("aggregate_apply_command_latency_seconds")
        .setSpanKind(SpanKind.INTERNAL)

    val persistEventsLatency = tracer.spanBuilder("aggregate_persist_events_latency_seconds")
        .setSpanKind(SpanKind.INTERNAL)

    override fun <C : DomainCommand, E : DomainEvent, S : AggregateState> supports(aggregateType: Aggregate<C, E, S>) =
        true

    override fun <C : DomainCommand, E : DomainEvent, S : AggregateState> createProbe(
        aggregateType: Aggregate<C, E, S>,
        aggregateId: AggregateId,
    ): CommandHandlingProbe<C, E, S> = OpenTelemetryCommandHandlingProbe(aggregateType)

    inner class OpenTelemetryCommandHandlingProbe<C : DomainCommand, E : DomainEvent, S : AggregateState>(private val aggregateType: Aggregate<C, E, S>) :
        CommandHandlingProbe<C, E, S> {

        private var commandName: String? = null

        private var commandHandlingTimerContext: Span? = null

        private var recoveringAggregateTimerContext: Span? = null

        private var applyCommandTimerContext: Span? = null

        private var persistEventsTimerContext: Span? = null

        override fun startedHandling(command: CommandEnvelope<C>) {
            commandName = command.command::class.simpleName
            commandHandlingTimerContext = commandHandlingTimerContext ?: commandExecutionLatency.startSpan()
                .setAttribute(aggregateTypeKey, aggregateType.blueprint.name)
                .setAttribute(commandTypeKey, command.command::class.simpleName)
            commandHandlingTimerContext?.makeCurrent()

        }

        override fun startedRecoveringAggregate() {
            recoveringAggregateTimerContext = recoveringAggregateTimerContext ?: aggregateRecoveryLatency.startSpan()
                .setAttribute(aggregateTypeKey, aggregateType.blueprint.name)

            recoveringAggregateTimerContext?.makeCurrent()
        }


        override fun startedApplyingCommand() {
            applyCommandTimerContext = applyCommandTimerContext ?: applyCommandLatency.startSpan()
                .setAttribute(aggregateTypeKey, aggregateType.blueprint.name)
            applyCommandTimerContext?.makeCurrent()
        }

        override fun startedPersistingEvents(events: List<E>, expectedSequenceNumber: Long) {
            persistEventsTimerContext = persistEventsTimerContext ?: persistEventsLatency.startSpan()
                .setAttribute(aggregateTypeKey, aggregateType.blueprint.name)
        }

        override fun finishedRecoveringAggregate(previousEvents: List<E>, version: Long, state: S?) {

            recoveringAggregateTimerContext?.end()
            recoveringAggregateTimerContext = null

        }

        override fun finishedRecoveringAggregate(unexpectedException: Throwable) {
            recoveringAggregateTimerContext?.end()
            recoveringAggregateTimerContext = null

        }

        override fun commandApplicationAccepted(events: List<E>, deduplicated: Boolean) {
            applyCommandTimerContext?.end()
            applyCommandTimerContext = null

        }

        override fun commandApplicationRejected(rejection: Throwable, deduplicated: Boolean) {
            applyCommandTimerContext?.end()
            applyCommandTimerContext = null

        }

        override fun commandApplicationFailed(unexpectedException: Throwable) {
            applyCommandTimerContext?.end()
            applyCommandTimerContext = null

        }

        override fun finishedPersistingEvents(persistedEvents: List<PersistedEvent<E>>) {
            persistEventsTimerContext?.end()
            persistEventsTimerContext = null
        }

        override fun finishedPersistingEvents(unexpectedException: Throwable) {

            persistEventsTimerContext?.end()
            persistEventsTimerContext = null

        }

        override fun finishedHandling(result: CommandHandlingResult<E>) {
            commandHandlingTimerContext?.end()
            commandHandlingTimerContext = null
        }
    }
}


