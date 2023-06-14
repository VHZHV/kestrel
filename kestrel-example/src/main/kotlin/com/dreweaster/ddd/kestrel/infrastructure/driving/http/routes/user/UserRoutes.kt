package com.dreweaster.ddd.kestrel.infrastructure.driving.http.routes.user

import com.dreweaster.ddd.kestrel.application.AggregateId
import com.dreweaster.ddd.kestrel.application.Backend
import com.dreweaster.ddd.kestrel.application.DomainModel
import com.dreweaster.ddd.kestrel.application.IdGenerator
import com.dreweaster.ddd.kestrel.application.SuccessResult
import com.dreweaster.ddd.kestrel.application.readmodel.user.UserDTO
import com.dreweaster.ddd.kestrel.application.readmodel.user.UserReadModel
import com.dreweaster.ddd.kestrel.domain.aggregates.user.RegisterUser
import com.dreweaster.ddd.kestrel.domain.aggregates.user.User
import com.dreweaster.ddd.kestrel.infrastructure.driving.http.routes.BaseRoutes
import com.dreweaster.ddd.kestrel.infrastructure.http.eventstream.producer.BoundedContextHttpJsonEventStreamProducer
import com.github.salomonbrys.kotson.jsonObject
import com.github.salomonbrys.kotson.string
import com.google.gson.JsonObject
import com.google.inject.Inject
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.Application
import io.ktor.server.application.call
import io.ktor.server.response.respond
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.route
import io.ktor.server.routing.routing
import io.ktor.util.toMap

class UserRoutes @Inject constructor(
    application: Application,
    domainModel: DomainModel,
    userReadModel: UserReadModel,
    backend: Backend,
) : BaseRoutes() {

    init {
        application.routing {

            route("/events") {

                get {
                    val producer = BoundedContextHttpJsonEventStreamProducer(backend)
                    call.respondWithJson(producer.produceFrom(call.parameters.toMap()))
                }
            }

            route("/users") {

                get {
                    call.respondWithJson(userReadModel.findAllUsers().map { userToJsonObject(it) })
                }

                post {
                    val request = RegisterUserRequest(call.receiveJson())
                    val user = domainModel.aggregateRootOf(User, request.id)

                    when (RegisterUser(request.username, request.password) sendTo user) {
                        is SuccessResult -> call.respondWithJson(jsonObject("id" to request.id.value))
                        else -> call.respond(HttpStatusCode.InternalServerError)
                    }
                }

                route("{id}") {
                    get {
                        val user = userReadModel.findUserById(call.parameters["id"]!!)
                        if (user == null) {
                            call.respond(HttpStatusCode.NotFound)
                        } else {
                            call.respondWithJson(
                                userToJsonObject(user),
                            )
                        }
                    }
                }
            }
        }
    }

    private fun userToJsonObject(user: UserDTO) =
        jsonObject(
            "id" to user.id,
            "username" to user.username,
            "password" to user.password,
            "locked" to user.locked,
        )
}

data class RegisterUserRequest(val id: AggregateId, val username: String, val password: String) {
    companion object {
        operator fun invoke(requestBody: JsonObject): RegisterUserRequest {
            val username = requestBody["username"].string
            val password = requestBody["password"].string
            return RegisterUserRequest(AggregateId(IdGenerator.randomId()), username, password)
        }
    }
}
