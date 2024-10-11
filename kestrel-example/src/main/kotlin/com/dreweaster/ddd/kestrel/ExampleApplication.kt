package com.dreweaster.ddd.kestrel

import com.dreweaster.ddd.kestrel.infrastructure.ExampleModule
import com.google.inject.Guice
import io.ktor.server.application.Application
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import org.flywaydb.core.Flyway

fun Application.module() {
    Guice.createInjector(ExampleModule(this))
}

fun main() {
    // Migrate DB
    Flyway.configure()
        .dataSource("jdbc:postgresql://example-db/postgres", "postgres", "password")
        .load()
        .migrate()

    embeddedServer(Netty).start()
}
