package com.dreweaster.ddd.kestrel.infrastructure.driving.http.routes

import com.dreweaster.ddd.kestrel.application.AggregateRoot
import com.dreweaster.ddd.kestrel.application.CommandHandlingResult
import com.dreweaster.ddd.kestrel.domain.DomainCommand
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import com.google.gson.Gson
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import io.ktor.http.ContentType
import io.ktor.server.application.ApplicationCall
import io.ktor.server.response.respondText
import io.ktor.utils.io.jvm.javaio.toInputStream
import java.io.InputStreamReader

abstract class BaseRoutes {

    companion object {
        val gson = Gson()
    }

    suspend fun ApplicationCall.respondWithJson(obj: Any) = respondText(gson.toJson(obj), ContentType.Application.Json)

    fun ApplicationCall.receiveJson() =
        JsonParser.parseReader(InputStreamReader(request.receiveChannel().toInputStream())) as JsonObject

    // Define an extension to Int
    suspend infix fun <C : DomainCommand, E : DomainEvent> C.sendTo(aggregateRoot: AggregateRoot<C, E>): CommandHandlingResult<E> =
        aggregateRoot.handleCommand(this)
}
