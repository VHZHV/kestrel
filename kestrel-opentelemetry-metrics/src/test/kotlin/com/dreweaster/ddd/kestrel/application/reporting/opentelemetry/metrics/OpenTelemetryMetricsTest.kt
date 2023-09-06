package com.dreweaster.ddd.kestrel.application.reporting.opentelemetry.metrics

import com.dreweaster.ddd.kestrel.application.AggregateId
import com.dreweaster.ddd.kestrel.application.CommandHandlingResult
import com.dreweaster.ddd.kestrel.application.DomainModel
import com.dreweaster.ddd.kestrel.application.EventPayloadMapper
import com.dreweaster.ddd.kestrel.application.EventSourcedDomainModel
import com.dreweaster.ddd.kestrel.application.TwentyFourHourWindowCommandDeduplication
import com.dreweaster.ddd.kestrel.application.eventstream.BoundedContextEventStreamSources
import com.dreweaster.ddd.kestrel.application.eventstream.BoundedContextName
import com.dreweaster.ddd.kestrel.application.eventstream.EventStreamSubscriptionEdenPolicy
import com.dreweaster.ddd.kestrel.application.eventstream.StatelessEventConsumer
import com.dreweaster.ddd.kestrel.domain.Aggregate
import com.dreweaster.ddd.kestrel.domain.AggregateBlueprint
import com.dreweaster.ddd.kestrel.domain.AggregateState
import com.dreweaster.ddd.kestrel.domain.DomainCommand
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import com.dreweaster.ddd.kestrel.domain.DomainEventTag
import com.dreweaster.ddd.kestrel.infrastructure.InMemoryBackend
import com.dreweaster.ddd.kestrel.infrastructure.SerialiseInMemoryEventStreamHandler
import com.dreweaster.ddd.kestrel.infrastructure.cluster.LocalClusterManager
import com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json.JsonEventMappingConfigurationFactory
import com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json.JsonEventMappingConfigurer
import com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json.JsonEventPayloadMapper
import com.dreweaster.ddd.kestrel.infrastructure.http.eventstream.consumer.BoundedContextHttpEventStreamSource
import com.dreweaster.ddd.kestrel.infrastructure.http.eventstream.consumer.BoundedContextHttpEventStreamSourceConfiguration
import com.dreweaster.ddd.kestrel.infrastructure.http.eventstream.consumer.BoundedContextHttpEventStreamSourceFactory
import com.dreweaster.ddd.kestrel.infrastructure.http.eventstream.consumer.offset.InMemoryOffsetManager
import com.dreweaster.ddd.kestrel.infrastructure.http.eventstream.producer.BoundedContextHttpJsonEventStreamProducer
import com.dreweaster.ddd.kestrel.infrastructure.job.ScheduledExecutorServiceJobManager
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.aResponse
import com.github.tomakehurst.wiremock.client.WireMock.get
import com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching
import com.github.tomakehurst.wiremock.core.WireMockConfiguration
import com.github.tomakehurst.wiremock.extension.ResponseTransformerV2
import com.github.tomakehurst.wiremock.http.Response
import com.github.tomakehurst.wiremock.stubbing.ServeEvent
import com.google.gson.Gson
import com.google.gson.JsonObject
import io.kotest.assertions.nondeterministic.eventually
import io.kotest.common.runBlocking
import io.kotest.core.spec.style.WordSpec
import io.kotest.matchers.string.shouldContain
import io.opentelemetry.exporter.prometheus.PrometheusHttpServer
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.resources.Resource
import org.asynchttpclient.DefaultAsyncHttpClient
import org.asynchttpclient.RequestBuilder
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.Executors
import kotlin.time.Duration.Companion.minutes

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
    boundedContexts: BoundedContextEventStreamSources,
) : StatelessEventConsumer(boundedContexts) {

    private val logger = LoggerFactory.getLogger(this::class.java)
    val events = mutableListOf<Event>()

    val mockServer = WireMockServer()

    init {

        consumer {
            subscribe(
                context = ProducingConsumingBoundedContext,
                subscriptionName = "self__events",
                edenPolicy = EventStreamSubscriptionEdenPolicy.BEGINNING_OF_TIME,
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

object ConfA : JsonEventMappingConfigurer<DomainEvent> {
    override fun configure(configurationFactory: JsonEventMappingConfigurationFactory<DomainEvent>) {
        configurationFactory.create(Event.A::class.qualifiedName!!)
            .mappingFunctions(serialise, deserialise)
    }

    val serialise: (DomainEvent) -> JsonObject = { JsonObject() }
    val deserialise: (JsonObject) -> Event.A = { Event.A }
}

object ConfB : JsonEventMappingConfigurer<DomainEvent> {
    override fun configure(configurationFactory: JsonEventMappingConfigurationFactory<DomainEvent>) {
        configurationFactory.create(Event.B::class.qualifiedName!!)
            .mappingFunctions(serialise, deserialise)
    }

    val serialise: (DomainEvent) -> JsonObject = { JsonObject() }
    val deserialise: (JsonObject) -> Event.B = { Event.B }
}

val eventPayloadMapper: EventPayloadMapper = JsonEventPayloadMapper(Gson(), listOf(ConfA, ConfB))
val backend = InMemoryBackend().also { be ->
    be.streamer = SerialiseInMemoryEventStreamHandler(eventPayloadMapper) {
        be.events
    }
}
val domain = EventSourcedDomainModel(backend, TwentyFourHourWindowCommandDeduplication)
val writeservice = EventWriteService(domain)
val producer = BoundedContextHttpJsonEventStreamProducer(backend)
val port = 9464
val prometheusHttpServer = PrometheusHttpServer.builder().setPort(port).build()
val resource = Resource.getDefault()
val meterProvider = SdkMeterProvider.builder()
    .setResource(resource)
    .registerMetricReader(prometheusHttpServer)
    .build()

val openTelemetry = OpenTelemetrySdk.builder()
    .setMeterProvider(
        meterProvider,
    ).build()

val eventStreamFactory = object : BoundedContextHttpEventStreamSourceFactory(ProducingConsumingBoundedContext) {
    override val mappers: EventMappers = eventMappers {

        tag(Event.tag.value) {
            event<Event.A>(
                Event.A::class.qualifiedName!!,
                ConfA.deserialise,
            )
            event<Event.B>(
                Event.B::class.qualifiedName!!,
                ConfB.deserialise,
            )
        }
    }
}

val config = object : BoundedContextHttpEventStreamSourceConfiguration {
    override val producerEndpointProtocol: String = "http"

    override val producerEndpointHostname: String = "localhost"

    override val producerEndpointPort: Int = 8080
    override val producerEndpointPath: String = "/metrics"

    override fun batchSizeFor(subscriptionName: String): Int = 10

    override fun repeatScheduleFor(subscriptionName: String): Duration = Duration.ofSeconds(1)

    override fun enabled(subscriptionName: String): Boolean = true
}
val httpClient = DefaultAsyncHttpClient()

val configuration = WireMockConfiguration().port(8080).extensions(object : ResponseTransformerV2 {
    override fun getName(): String = "producing-events"

    override fun transform(resp: Response, event: ServeEvent): Response {
        val params = listOf(
            "tags",
            "after_timestamp",
            "after_offset",
            "batch_size",
        ).mapNotNull {
            try {
                it to event.request.queryParameter(it).values()
            } catch (e: Exception) {
                null
            }
        }.toMap()

        return runBlocking {
            val result = producer.produceFrom(params)
            Response.response()
                .status(200)
                .body(result.toString())
                .build()
        }
    }
})

class OpenTelemetryMetricsTest : WordSpec({

    val mockServer = WireMockServer(configuration)
    beforeSpec {

        mockServer.start()
        OpenTelemetryMetricsDomainModelReporter(openTelemetry).also {
            domain.addReporter(it)
        }
        mockServer.stubFor(
            get(urlPathMatching(".+"))
                .willReturn(
                    aResponse()
                        .withTransformers("producing-events"),
                ),
        )
    }
    beforeTest {
        backend.clear()
    }
    afterSpec {
        mockServer.stop()
    }

    "domain metrics publication" should {

        beforeTest {
            writeservice.doA("1")
            writeservice.doB("1")
            writeservice.doA("2")
            writeservice.doA("2")
        }
        "publish the correct metrics" {

            eventually(1.minutes) {
                val responseBody =
                    httpClient.executeRequest(RequestBuilder().setUrl("http://localhost:$port/metrics").build())
                        .get().responseBody

                responseBody shouldContain "aggregate_persist_events_total"
                responseBody shouldContain "aggregate_command_execution_total"
                responseBody shouldContain "aggregate_apply_command_total"
                responseBody shouldContain "aggregate_events_emitted_total"
                responseBody shouldContain "aggregate_recovery_total"
            }
        }
    }
    "consumer metrics publication" should {

        lateinit var source: BoundedContextHttpEventStreamSource
        beforeTest {

            val eventStreamReporter = OpenTelemetryMetricsBoundedContextHttpEventStreamSourceReporter(
                openTelemetry,
                ProducingConsumingBoundedContext,
            )

            source = eventStreamFactory.createHttpEventStreamSource(
                httpClient,
                config,
                InMemoryOffsetManager,
                ScheduledExecutorServiceJobManager(
                    LocalClusterManager,
                    Executors.newSingleThreadScheduledExecutor(),
                ),
            ).also {
                it.addReporter(eventStreamReporter)
            }
            val sources = BoundedContextEventStreamSources(listOf(ProducingConsumingBoundedContext to source))

            CycleEventConsumer(sources)

            writeservice.doA("1")
            writeservice.doB("1")
            writeservice.doA("2")
            writeservice.doA("2")
        }
        "publish the correct metrics" {
            eventually(1.minutes) {
                val responseBody =
                    httpClient.executeRequest(RequestBuilder().setUrl("http://localhost:$port/metrics").build())
                        .get().responseBody

                responseBody shouldContain "max_offset_events"
                responseBody shouldContain "event_handled_total"
                responseBody shouldContain "offset_retrievals_total"
                responseBody shouldContain "consumption_attempted_total"
                responseBody shouldContain "offset_stores_total"
                responseBody shouldContain "current_offset_latest_events"
            }
        }
    }
})
