package com.dreweaster.ddd.kestrel.application.consumers

import com.dreweaster.ddd.kestrel.application.BoundedContextEventSources
import com.dreweaster.ddd.kestrel.application.BoundedContextSubscriptionEdenPolicy.FROM_NOW
import com.dreweaster.ddd.kestrel.domain.aggregates.user.UserRegistered
import com.dreweaster.ddd.kestrel.application.consumers.BoundedContexts.UserContext
import com.dreweaster.ddd.kestrel.application.StatelessProcessManager
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono.*

class HelloNewUser constructor(boundedContexts: BoundedContextEventSources): StatelessProcessManager(boundedContexts) {

    private val LOG = LoggerFactory.getLogger(HelloNewUser::class.java)

    init {
        processManager(name = "hello-new-user") {

            subscribe(context = UserContext, edenPolicy = FROM_NOW) {

                event<UserRegistered> { event, _ ->
                    fromRunnable<Unit> { LOG.info("Hello ${event.username}!") }
                }
            }
        }.start()
    }
}