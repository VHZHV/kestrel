package com.dreweaster.ddd.kestrel.application.reporting.opentelemetry.metrics

import com.dreweaster.ddd.kestrel.application.*
import com.dreweaster.ddd.kestrel.application.eventstream.BoundedContextEventStreamSources
import com.dreweaster.ddd.kestrel.application.eventstream.BoundedContextName
import com.dreweaster.ddd.kestrel.application.eventstream.EventStreamSubscriptionEdenPolicy
import com.dreweaster.ddd.kestrel.application.eventstream.StatelessEventConsumer
import com.dreweaster.ddd.kestrel.domain.*
import com.dreweaster.ddd.kestrel.infrastructure.InMemoryBackend
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock
import com.github.tomakehurst.wiremock.matching.EqualToPattern
import io.kotest.assertions.timing.eventually
import io.kotest.core.spec.style.WordSpec
import io.kotest.matchers.shouldBe
import org.slf4j.LoggerFactory
import java.nio.file.Paths
import kotlin.time.Duration.Companion.hours


class EventWriteService(val domainModel: DomainModel) {

    suspend fun doA(id: String): CommandHandlingResult<Event> {
        return domainModel.aggregateRootOf(Cycle, AggregateId(id)).handleCommand(Command.A)
    }

    suspend fun doB(id: String): CommandHandlingResult<Event> {
        return domainModel.aggregateRootOf(Cycle, AggregateId(id)).handleCommand(Command.B)
    }

}

sealed interface Event : DomainEvent {
    override val tag: DomainEventTag
        get() = Companion.tag

    object A : Event
    object B : Event

    companion object {
        val tag = DomainEventTag("public")
    }
}

sealed interface Command : DomainCommand {
    object A : Command
    object B : Command
}

sealed interface State : AggregateState {
    object A : State
    object B : State
}

object Cycle : Aggregate<Command, Event, State> {
    override val blueprint: AggregateBlueprint<Command, Event, State> = aggregate("cycle") {
        edenBehaviour {

            receive {
                command<Command.A> {
                    accept(Event.A)
                }
            }

            apply {
                event<Event.A> { State.A }
            }
        }
        behaviour<State.A> {
            receive {
                command<Command.B> { _, _ ->
                    accept(Event.B)
                }
            }
            apply {
                event<Event.B> { _, _ -> State.B }
            }
        }
    }
}


object ProducingConsumingBoundedContext : BoundedContextName {
    override val name: String = "producing_consuming"
}

class CycleEventConsumer(
    boundedContexts: BoundedContextEventStreamSources
) : StatelessEventConsumer(boundedContexts) {

    private val logger = LoggerFactory.getLogger(this::class.java)
    val events = mutableListOf<Event>()


    val mockServer = WireMockServer()

    init {

        consumer {
            subscribe(
                context = ProducingConsumingBoundedContext,
                subscriptionName = "self__events",
                edenPolicy = EventStreamSubscriptionEdenPolicy.BEGINNING_OF_TIME
            ) {
                event<Event.A> { e, _ ->
                    events.add(e)
                }
                event<Event.B> { e, _ ->
                    events.add(e)
                }
            }
        }.start()
    }
}

class ProducingConsumingMetricsTest : WordSpec({


    val mockServer = WireMockServer()

    mockServer.stubFor(
        WireMock.get(WireMock.urlPathMatching("/events"))
            .withQueryParam("after_offset", EqualToPattern("-1"))
            .willReturn(

                WireMock.aResponse()
                    .withStatus(200)
                    .withBody(


                    ),
            ),
    )


    val backend = InMemoryBackend()
    val domain = EventSourcedDomainModel(backend, TwentyFourHourWindowCommandDeduplication)
    val writeservice = EventWriteService(domain)
    println(Paths.get(".").toAbsolutePath().toString())

    beforeSpec {
        backend.clear()
        writeservice.doA("1")
        writeservice.doB("1")
        writeservice.doA("2")
        writeservice.doA("2")

    }

    "producer metrics publication" should {

        "publish the correct values".config(enabled = false) {


            eventually(1.hours) {

                true shouldBe false
            }


        }
        "publish with appropriate tags" {

        }
    }
    "consumer metrics publication" should {
        "publish the correct amount" {

        }
        "publish with appropriate tags" {

        }
    }

})

