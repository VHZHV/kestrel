package com.dreweaster.ddd.kestrel.domain

import com.dreweaster.ddd.kestrel.application.UnsupportedEventInCurrentBehaviour
import com.dreweaster.ddd.kestrel.application.UnsupportedEventInEdenBehaviour
import io.vavr.control.Try
import kotlin.reflect.KClass

data class DomainEventTag(val value: String)

interface DomainEvent {
    val tag: DomainEventTag
}

interface DomainCommand

interface AggregateState

interface Aggregate<C : DomainCommand, E : DomainEvent, S : AggregateState> {
    val blueprint: AggregateBlueprint<C, E, S>

    fun aggregate(name: String, init: AggregateBlueprint<C, E, S>.() -> Unit): AggregateBlueprint<C, E, S> {
        val aggregate = AggregateBlueprint<C, E, S>(name)
        aggregate.init()
        return aggregate
    }
}

class AggregateBlueprint<C : DomainCommand, E : DomainEvent, S : AggregateState>(val name: String) {
    var capturedEden: EdenBehaviour<C, E, S>? = null

    var capturedBehaviours: Map<KClass<S>, Behaviour<C, E, S, *>> = emptyMap()

    inline fun edenBehaviour(init: EdenBehaviour<C, E, S>.() -> Unit): EdenBehaviour<C, E, S> {
        val eden = EdenBehaviour<C, E, S>()
        eden.init()
        capturedEden = eden
        return eden
    }

    @Suppress("UNCHECKED_CAST")
    inline fun <reified State : S> behaviour(init: Behaviour<C, E, S, State>.() -> Unit): Behaviour<C, E, S, State> {
        val behaviour = Behaviour<C, E, S, State>()
        behaviour.init()
        capturedBehaviours += State::class as KClass<S> to behaviour
        return behaviour
    }

    val edenEventHandler: EdenEventHandler<E, S> =
        object : EdenEventHandler<E, S> {
            override fun canHandle(t: E) = capturedEden?.capturedApply?.capturedHandlers?.get(t::class) != null

            override fun invoke(t: E): S {
                if (!canHandle(t)) throw UnsupportedEventInEdenBehaviour
                return capturedEden!!.capturedApply!!.capturedHandlers[t::class]!!.invoke(t)
            }
        }

    val edenCommandHandler: EdenCommandHandler<C, Try<List<E>>> =
        object : EdenCommandHandler<C, Try<List<E>>> {
            override fun canHandle(t: C) = capturedEden?.capturedReceive?.capturedHandlers?.get(t::class) != null

            override fun options(t: C) = capturedEden!!.capturedReceive!!.capturedHandlers[t::class]!!.second

            override fun invoke(t: C): Try<List<E>> {
                if (!canHandle(t)) throw UnsupportedOperationException()
                return capturedEden!!
                    .capturedReceive!!
                    .capturedHandlers[t::class]!!
                    .first
                    .invoke(t)
            }
        }

    val commandHandler: Handler<S, C, Try<List<E>>> =
        object : Handler<S, C, Try<List<E>>> {
            override fun canHandle(t1: S, t2: C) = capturedBehaviours[t1::class]?.capturedReceive?.capturedHandlers?.get(t2::class) != null

            override fun invoke(t1: S, t2: C): Try<List<E>> {
                if (!canHandle(t1, t2)) throw UnsupportedOperationException()
                return capturedBehaviours[t1::class]
                    ?.capturedReceive
                    ?.capturedHandlers
                    ?.get(t2::class)
                    ?.invoke(t1, t2)!!
            }
        }

    val eventHandler: Handler<S, E, S> =
        object : Handler<S, E, S> {
            override fun canHandle(t1: S, t2: E) = capturedBehaviours[t1::class]?.capturedApply?.capturedHandlers?.get(t2::class) != null

            override fun invoke(t1: S, t2: E): S {
                if (!canHandle(t1, t2)) throw UnsupportedEventInCurrentBehaviour
                return capturedBehaviours[t1::class]
                    ?.capturedApply
                    ?.capturedHandlers
                    ?.get(t2::class)
                    ?.invoke(t1, t2)!!
            }
        }
}

class EdenBehaviour<C : DomainCommand, E : DomainEvent, S : AggregateState> {
    var capturedReceive: EdenReceive<C, E>? = null

    var capturedApply: EdenApply<E, S>? = null

    fun receive(init: EdenReceive<C, E>.() -> Unit): EdenReceive<C, E> {
        val receive = EdenReceive<C, E>()
        receive.init()
        capturedReceive = receive
        return receive
    }

    fun apply(init: EdenApply<E, S>.() -> Unit): EdenApply<E, S> {
        val apply = EdenApply<E, S>()
        apply.init()
        capturedApply = apply
        return apply
    }
}

data class EdenCommandOptions(val allowInAllBehaviours: Boolean)

class EdenReceive<C : DomainCommand, E : DomainEvent> {
    var capturedHandlers: Map<KClass<C>, Pair<(C) -> Try<List<E>>, EdenCommandOptions>> = emptyMap()

    @Suppress("UNCHECKED_CAST")
    inline fun <reified Cmd : C> command(allowInAllBehaviours: Boolean = false, noinline handler: (Cmd) -> Try<List<E>>) {
        capturedHandlers += Cmd::class as KClass<C> to
            Pair(
                handler as (C) -> Try<List<E>>,
                EdenCommandOptions(allowInAllBehaviours),
            )
    }

    fun <Evt : E> accept(vararg event: Evt): Try<List<Evt>> = Try.success(event.toList())

    fun <Evt : E> reject(error: Throwable): Try<List<Evt>> = Try.failure(error)
}

class EdenApply<E : DomainEvent, S : AggregateState> {
    var capturedHandlers: Map<KClass<E>, ((E) -> S)> = emptyMap()

    @Suppress("UNCHECKED_CAST")
    inline fun <reified Evt : E> event(noinline handler: (Evt) -> S) {
        capturedHandlers += Evt::class as KClass<E> to handler as (E) -> S
    }
}

class Behaviour<C : DomainCommand, E : DomainEvent, S : AggregateState, State : S> {
    var capturedReceive: Receive<C, E, S, State>? = null

    var capturedApply: Apply<E, S, State>? = null

    fun receive(init: Receive<C, E, S, State>.() -> Unit): Receive<C, E, S, State> {
        val receive = Receive<C, E, S, State>()
        receive.init()
        capturedReceive = receive
        return receive
    }

    fun apply(init: Apply<E, S, State>.() -> Unit): Apply<E, S, State> {
        val apply = Apply<E, S, State>()
        apply.init()
        capturedApply = apply
        return apply
    }
}

class Receive<C : DomainCommand, E : DomainEvent, S : AggregateState, out State : S> {
    var capturedHandlers: Map<KClass<C>, ((S, C) -> Try<List<E>>)> = emptyMap()

    @Suppress("UNCHECKED_CAST")
    inline fun <reified Cmd : C> command(noinline handler: (State, Cmd) -> Try<List<E>>) {
        capturedHandlers += Cmd::class as KClass<C> to handler as (S, C) -> Try<List<E>>
    }

    fun accept(vararg event: E): Try<List<E>> = Try.success(event.toList())

    fun reject(error: Throwable): Try<List<E>> = Try.failure(error)
}

class Apply<E : DomainEvent, S : AggregateState, out State : S> {
    var capturedHandlers: Map<KClass<E>, ((S, E) -> S)> = emptyMap()

    @Suppress("UNCHECKED_CAST")
    inline fun <reified Evt : E> event(noinline handler: (State, Evt) -> S) {
        capturedHandlers += Evt::class as KClass<E> to handler as (S, E) -> S
    }
}

interface Handler<T1, T2, R> {
    fun canHandle(t1: T1, t2: T2): Boolean

    operator fun invoke(t1: T1, t2: T2): R
}

interface EdenEventHandler<T, R> {
    fun canHandle(t: T): Boolean

    operator fun invoke(t: T): R
}

interface EdenCommandHandler<T, R> {
    fun canHandle(t: T): Boolean

    fun options(t: T): EdenCommandOptions

    operator fun invoke(t: T): R
}
