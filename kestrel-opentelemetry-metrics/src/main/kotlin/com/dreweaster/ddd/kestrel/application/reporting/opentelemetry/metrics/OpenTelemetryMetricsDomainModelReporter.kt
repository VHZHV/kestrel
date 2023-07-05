package com.dreweaster.ddd.kestrel.application.reporting.opentelemetry.metrics

import com.dreweaster.ddd.kestrel.application.*
import com.dreweaster.ddd.kestrel.domain.Aggregate
import com.dreweaster.ddd.kestrel.domain.AggregateState
import com.dreweaster.ddd.kestrel.domain.DomainCommand
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.common.AttributeKey.stringKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.metrics.LongCounter
import io.opentelemetry.api.trace.SpanKind
import io.opentelemetry.api.trace.Tracer

class OpenTelemetryMetricsDomainModelReporter(openTelemetry: OpenTelemetry) : DomainModelReporter {
    val aggregateTypeKey = "aggregate_type"
    val eventTypeKey = "event_type"
    val commandTypeKey = "command_type"
    val resultKey = "result"
    val deduplicatedKey = "deduplicated"

    private val tracer: Tracer = openTelemetry.getTracer("com.dreweaster.ddd.kestrel.DomainModelReporter")


    val commandExecutionLatency = tracer.spanBuilder("aggregate_command_execution_latency_seconds")
        .setSpanKind(SpanKind.INTERNAL)

    var meter = openTelemetry.meterBuilder("com.dreweaster.ddd.kestrel.DomainModelReporter")
        .build()

    var commandExecution: LongCounter = meter
        .counterBuilder("aggregate_command_execution_total")
        .setDescription("Total aggregate commands executed")
        .setUnit("1")
        .build()

    var eventsEmitted: LongCounter = meter
        .counterBuilder("aggregate_events_emitted_total")
        .setDescription("Total aggregate events emitted")
        .setUnit("1")
        .build()

    var aggregateRecovery: LongCounter = meter
        .counterBuilder("aggregate_recovery_total")
        .setDescription("Total aggregates recovered")
        .setUnit("1")
        .build()

    var applyCommand: LongCounter = meter
        .counterBuilder("aggregate_apply_command_total")
        .setDescription("Total aggregate commands applied")
        .setUnit("1")
        .build()

    var persistEvents: LongCounter = meter
        .counterBuilder("aggregate_persist_events_total")
        .setDescription("Total calls to persist events for aggregate")
        .setUnit("1")
        .build()


    override fun <C : DomainCommand, E : DomainEvent, S : AggregateState> supports(aggregateType: Aggregate<C, E, S>) =
        true

    override fun <C : DomainCommand, E : DomainEvent, S : AggregateState> createProbe(
        aggregateType: Aggregate<C, E, S>,
        aggregateId: AggregateId,
    ): CommandHandlingProbe<C, E, S> = OpenTelemetryCommandHandlingProbe(aggregateType)

    inner class OpenTelemetryCommandHandlingProbe<C : DomainCommand, E : DomainEvent, S : AggregateState>(private val aggregateType: Aggregate<C, E, S>) :
        CommandHandlingProbe<C, E, S> {

        private var commandName: String? = null


        override fun startedHandling(command: CommandEnvelope<C>) {


        }

        override fun startedRecoveringAggregate() {

        }


        override fun startedApplyingCommand() {

        }

        override fun startedPersistingEvents(events: List<E>, expectedSequenceNumber: Long) {

        }

        override fun finishedRecoveringAggregate(previousEvents: List<E>, version: Long, state: S?) {

            aggregateRecovery.add(
                1L,
                Attributes.of(
                    stringKey(aggregateTypeKey), aggregateType.blueprint.name,
                    stringKey(resultKey), "success"
                )
            )
        }

        override fun finishedRecoveringAggregate(unexpectedException: Throwable) {
            aggregateRecovery.add(
                1L,
                Attributes.of(
                    stringKey(aggregateTypeKey), aggregateType.blueprint.name,
                    stringKey(resultKey), "failure"
                )
            )
        }

        override fun commandApplicationAccepted(events: List<E>, deduplicated: Boolean) {
            applyCommand.add(
                1L,
                Attributes.of(
                    stringKey(aggregateTypeKey), aggregateType.blueprint.name,
                    stringKey(resultKey), "accepted",
                    stringKey(deduplicatedKey), deduplicated.toString()
                )
            )
        }

        override fun commandApplicationRejected(rejection: Throwable, deduplicated: Boolean) {
            applyCommand.add(
                1L,
                Attributes.of(
                    stringKey(aggregateTypeKey), aggregateType.blueprint.name,
                    stringKey(resultKey), "rejected",
                    stringKey(deduplicatedKey), deduplicated.toString()
                )
            )
        }

        override fun commandApplicationFailed(unexpectedException: Throwable) {
            applyCommand.add(
                1L,
                Attributes.of(
                    stringKey(aggregateTypeKey), aggregateType.blueprint.name,
                    stringKey(resultKey), "rejected",
                    stringKey(deduplicatedKey), "false"
                )
            )
        }

        override fun finishedPersistingEvents(persistedEvents: List<PersistedEvent<E>>) {
            persistEvents.add(
                1L,
                Attributes.of(
                    stringKey(aggregateTypeKey), aggregateType.blueprint.name,
                    stringKey(resultKey), "success",
                )
            )
        }

        override fun finishedPersistingEvents(unexpectedException: Throwable) {
            persistEvents.add(
                1L,
                Attributes.of(
                    stringKey(aggregateTypeKey), aggregateType.blueprint.name,
                    stringKey(resultKey), "failure",
                )
            )

        }

        override fun finishedHandling(result: CommandHandlingResult<E>) {

            val useCommand = commandName ?: "Unkown"
            when (result) {
                is SuccessResult<E> -> {

                    commandExecution.add(
                        1L,
                        Attributes.of(
                            stringKey(aggregateTypeKey), aggregateType.blueprint.name,
                            stringKey(commandTypeKey), useCommand,
                            stringKey(resultKey), "accepted",
                            stringKey(deduplicatedKey), result.deduplicated.toString()
                        )
                    )

                    result.generatedEvents.forEach {
                        eventsEmitted.add(
                            1L,
                            Attributes.of(
                                stringKey(aggregateTypeKey), aggregateType.blueprint.name,
                                stringKey(eventTypeKey), it::class.simpleName
                            )
                        )
                    }
                }

                is RejectionResult<E> -> {
                    commandExecution.add(
                        1L,
                        Attributes.of(
                            stringKey(aggregateTypeKey), aggregateType.blueprint.name,
                            stringKey(commandTypeKey), useCommand,
                            stringKey(resultKey), "rejected",
                            stringKey(deduplicatedKey), result.deduplicated.toString()
                        )
                    )
                }

                is ConcurrentModificationResult<E> -> {

                    commandExecution.add(
                        1L,
                        Attributes.of(
                            stringKey(aggregateTypeKey), aggregateType.blueprint.name,
                            stringKey(commandTypeKey), useCommand,
                            stringKey(resultKey), "failed-concurrent-modification",
                            stringKey(deduplicatedKey), "na"
                        )
                    )
                }

                is UnexpectedExceptionResult<E> -> {

                    commandExecution.add(
                        1L,
                        Attributes.of(
                            stringKey(aggregateTypeKey), aggregateType.blueprint.name,
                            stringKey(commandTypeKey), useCommand,
                            stringKey(resultKey), "failed-unexpected-exception",
                            stringKey(deduplicatedKey), "na"
                        )
                    )
                }
            }
        }
    }
}

