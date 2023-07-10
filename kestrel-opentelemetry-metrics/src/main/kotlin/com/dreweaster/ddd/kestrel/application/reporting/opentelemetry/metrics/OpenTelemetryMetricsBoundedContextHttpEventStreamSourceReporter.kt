package com.dreweaster.ddd.kestrel.application.reporting.opentelemetry.metrics

import com.dreweaster.ddd.kestrel.application.eventstream.BoundedContextName
import com.dreweaster.ddd.kestrel.infrastructure.http.eventstream.consumer.reporting.BoundedContextHttpEventStreamSourceProbe
import com.dreweaster.ddd.kestrel.infrastructure.http.eventstream.consumer.reporting.BoundedContextHttpEventStreamSourceReporter
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.metrics.LongCounter
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class OpenTelemetryMetricsBoundedContextHttpEventStreamSourceReporter constructor(
    openTelemetry: OpenTelemetry,
    private val context: BoundedContextName,
) : BoundedContextHttpEventStreamSourceReporter {

    private val meter = openTelemetry.meterBuilder("com.dreweaster.ddd.kestrel.BoundedContextHttpEventStreamSourceReporter")
        .build()

    val consumptionAttemptMeter: LongCounter = meter
        .counterBuilder("consumption_attempted")
        .setDescription("An attempt to read from the event stream")
        .setUnit("1")
        .build()

    val eventHandledMeter: LongCounter = meter
        .counterBuilder("event_handled")
        .setDescription("An attempt to handle an event")
        .setUnit("1")
        .build()

    val maxOffsetMeter: LongCounter = meter
        .counterBuilder("max_offset")
        .setDescription("The maximum offset available for a consumer to consume to")
        .setUnit("events")
        .build()

    val currentOffsetMeter: LongCounter = meter
        .counterBuilder("current_offset_latest")
        .setDescription("Current offset a consumer has reached")
        .setUnit("events")
        .build()

    val offsetRetrievalMeter: LongCounter = meter
        .counterBuilder("offset_retrievals")
        .setDescription("Attempts to retrieve current stream's offset")
        .setUnit("1")
        .build()

    val offsetStorageMeter: LongCounter = meter
        .counterBuilder("offset_stores")
        .setDescription("Attempts to store current stream's offset")
        .setUnit("1")
        .build()

    private val logger: Logger = LoggerFactory.getLogger(BoundedContextHttpEventStreamSourceReporter::class.java)

    override fun createProbe(subscriberName: String): BoundedContextHttpEventStreamSourceProbe =
        OpenCensusBoundedContextHttpEventStreamSourceProbe(subscriberName)

    init {
        logger.info("Initialising Metrics")
    }

    inner class OpenCensusBoundedContextHttpEventStreamSourceProbe(
        private val subscriberName: String,
    ) : BoundedContextHttpEventStreamSourceProbe {

        private fun baseAttributes() = Attributes.builder()
            .put("subscription", subscriberName)
            .put("context", context.name)

        private val successAttributes = baseAttributes()
            .put("result", "success")

        private val failureAttributes = baseAttributes()
            .put("result", "failure")

        override fun startedHandlingEvent(eventType: String) {
        }

        override fun finishedHandlingEvent() {
            eventHandledMeter.add(1, successAttributes.build())
        }

        override fun finishedHandlingEvent(ex: Throwable) {
            eventHandledMeter.add(1, failureAttributes.build())
        }

        override fun startedConsuming() {}
        override fun finishedConsuming() {
            consumptionAttemptMeter.add(1, successAttributes.build())
        }
        override fun finishedConsuming(ex: Throwable) {
            consumptionAttemptMeter.add(1, failureAttributes.build())
        }
        override fun startedFetchingEventStream() {}
        override fun finishedFetchingEventStream(maxOffset: Long) {
            maxOffsetMeter.add(maxOffset, baseAttributes().build())
        }

        override fun finishedFetchingEventStream(ex: Throwable) {
        }

        override fun startedFetchingOffset() {}
        override fun finishedFetchingOffset() {
            offsetRetrievalMeter.add(1, successAttributes.build())
        }
        override fun finishedFetchingOffset(ex: Throwable) {
            offsetRetrievalMeter.add(1, failureAttributes.build())
        }
        override fun startedSavingOffset() {}
        override fun finishedSavingOffset(offset: Long) {
            // Sometimes reported as -1 (if value is unknown), but this isn't helpful to record
            if (offset >= 0) {
                currentOffsetMeter.add(offset, baseAttributes().build())
            }
            offsetStorageMeter.add(1, successAttributes.build())
        }

        override fun finishedSavingOffset(ex: Throwable) {
            offsetRetrievalMeter.add(1, failureAttributes.build())
        }
    }
}
