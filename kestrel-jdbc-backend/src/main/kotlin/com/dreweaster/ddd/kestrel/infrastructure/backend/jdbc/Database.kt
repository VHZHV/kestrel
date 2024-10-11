package com.dreweaster.ddd.kestrel.infrastructure.backend.jdbc

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.withContext
import org.jetbrains.exposed.sql.Column
import org.jetbrains.exposed.sql.ColumnType
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.IColumnType
import org.jetbrains.exposed.sql.Index
import org.jetbrains.exposed.sql.Op
import org.jetbrains.exposed.sql.QueryBuilder
import org.jetbrains.exposed.sql.SqlExpressionBuilder
import org.jetbrains.exposed.sql.Table
import org.jetbrains.exposed.sql.Transaction
import org.jetbrains.exposed.sql.jodatime.DateColumnType
import org.jetbrains.exposed.sql.statements.InsertStatement
import org.jetbrains.exposed.sql.transactions.TransactionManager
import org.jetbrains.exposed.sql.transactions.transaction
import java.time.Instant
import javax.sql.DataSource
import java.time.Instant as JavaInstant
import org.joda.time.DateTime as JodaDateTime

class Database(dataSource: DataSource, private val context: CoroutineDispatcher) {
    private val db = Database.connect(dataSource)

    suspend fun <T> transaction(block: (DatabaseTransaction) -> T): T = withContext(context) {
        transaction(db) {
            block(
                object : DatabaseTransaction {
                    override fun rollback(): Unit = throw DatabaseTransaction.TransactionRollbackException

                    override fun rollback(throwable: Throwable): Unit = throw throwable
                },
            )
        }
    }
}

class UnexpectedNumberOfRowsAffectedInUpdate : RuntimeException()

interface DatabaseTransaction {
    object TransactionRollbackException : RuntimeException()

    fun rollback()

    fun rollback(throwable: Throwable)
}

fun Table.instant(name: String): Column<Instant> = registerColumn(name, InstantColumnType(true))

private fun JodaDateTime.toInstantJava() = JavaInstant.ofEpochMilli(this.millis)

private fun JavaInstant.toJodaDateTime() = JodaDateTime(this.toEpochMilli())

class InstantColumnType(time: Boolean) : ColumnType<Instant>() {
    private val delegate = DateColumnType(time)

    override fun sqlType(): String = delegate.sqlType()

    override fun nonNullValueToString(value: Instant): String = delegate.nonNullValueToString(value.toJodaDateTime())

    override fun valueFromDB(value: Any): Instant {
        val fromDb =
            when (value) {
                is JavaInstant -> delegate.valueFromDB(value.toJodaDateTime())
                else -> delegate.valueFromDB(value)
            }
        return when (fromDb) {
            is JodaDateTime -> fromDb.toInstantJava()
            else -> error("failed to convert value to Instant")
        }
    }

    override fun notNullValueToDB(value: Instant): Any = delegate.notNullValueToDB(value.toJodaDateTime())
}

sealed class ConflictTarget(val name: String, val columns: List<Column<*>>) {
    abstract fun toSql(): String
}

class PrimaryKeyConstraintTarget(table: Table, columns: List<Column<*>>) : ConflictTarget("${table.nameInDatabaseCase()}_pkey", columns) {
    override fun toSql() = "ON CONFLICT ON CONSTRAINT $name"
}

class ColumnTarget(column: Column<*>) : ConflictTarget(column.name, listOf(column)) {
    override fun toSql() = "ON CONFLICT($name)"
}

class IndexTarget(index: Index) : ConflictTarget(index.indexName, index.columns) {
    override fun toSql() = "ON CONFLICT($name)"
}

class UpsertStatement<Key : Any>(table: Table, private val conflictTarget: ConflictTarget, private val where: Op<Boolean>? = null) :
    InsertStatement<Key>(table, false) {
    override fun prepareSQL(transaction: Transaction, prepared: Boolean) = buildString {
        append(super.prepareSQL(transaction, prepared))
        append(" ")
        append(conflictTarget.toSql())
        append(" DO UPDATE SET ")
        values.keys
            .filter { it !in conflictTarget.columns }
            .joinTo(this) { "${transaction.identity(it)}=EXCLUDED.${transaction.identity(it)}" }

        where?.let { append(" WHERE ${QueryBuilder(true).append(it)}") }
    }

    override fun arguments(): List<List<Pair<IColumnType<*>, Any?>>> {
        val superArgs = super.arguments().first()

        QueryBuilder(true).run {
            where?.toQueryBuilder(this)
            return listOf(superArgs + args)
        }
    }
}

class InsertOnConflictDoNothingStatement<Key : Any>(table: Table, private val conflictTarget: ConflictTarget) :
    InsertStatement<Key>(table, false) {
    override fun prepareSQL(transaction: Transaction, prepared: Boolean) = buildString {
        append(super.prepareSQL(transaction, prepared))
        append(" ")
        append(conflictTarget.toSql())
        append(" DO NOTHING")
    }
}

fun <T : Table> T.insertOnConflictDoNothing(
    conflictTarget: ConflictTarget,
    body: T.(InsertOnConflictDoNothingStatement<Number>) -> Unit,
): Int {
    val query = InsertOnConflictDoNothingStatement<Number>(this, conflictTarget)
    body(query)
    return query.execute(TransactionManager.current())!!
}

fun <T : Table> T.upsert(
    conflictTarget: ConflictTarget,
    where: (SqlExpressionBuilder.() -> Op<Boolean>)? = null,
    body: T.(UpsertStatement<Number>) -> Unit,
): Int {
    val query = UpsertStatement<Number>(this, conflictTarget, where?.let { SqlExpressionBuilder.it() })
    body(query)
    return query.execute(TransactionManager.current())!!
}

fun Table.indexR(customIndexName: String? = null, isUnique: Boolean = false, vararg columns: Column<*>): Index {
    index(customIndexName = customIndexName, isUnique = isUnique, columns = columns)
    return indices[indices.size - 1]
}

fun Table.uniqueIndexR(customIndexName: String? = null, vararg columns: Column<*>): Index = indexR(customIndexName, true, *columns)

fun Table.primaryKeyConstraintConflictTarget(vararg columns: Column<*>): ConflictTarget = PrimaryKeyConstraintTarget(this, columns.toList())

fun Table.uniqueIndexConflictTarget(customIndexName: String? = null, vararg columns: Column<*>): IndexTarget =
    IndexTarget(uniqueIndexR(customIndexName, *columns))
