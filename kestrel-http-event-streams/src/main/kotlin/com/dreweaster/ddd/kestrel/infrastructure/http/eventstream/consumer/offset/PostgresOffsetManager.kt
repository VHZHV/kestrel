package com.dreweaster.ddd.kestrel.infrastructure.http.eventstream.consumer.offset

import com.dreweaster.ddd.kestrel.infrastructure.backend.jdbc.Database
import com.dreweaster.ddd.kestrel.infrastructure.backend.jdbc.UnexpectedNumberOfRowsAffectedInUpdate
import com.dreweaster.ddd.kestrel.infrastructure.backend.jdbc.primaryKeyConstraintConflictTarget
import com.dreweaster.ddd.kestrel.infrastructure.backend.jdbc.upsert
import com.dreweaster.ddd.kestrel.infrastructure.http.eventstream.consumer.offset.PostgresOffsetManager.Offsets.lastProcessedOffset
import com.dreweaster.ddd.kestrel.infrastructure.http.eventstream.consumer.offset.PostgresOffsetManager.Offsets.name
import com.dreweaster.ddd.kestrel.infrastructure.http.eventstream.consumer.offset.PostgresOffsetManager.Offsets.primaryKeyConstraintConflictTarget
import org.jetbrains.exposed.sql.Table
import org.jetbrains.exposed.sql.select

class PostgresOffsetManager(private val database: Database) : OffsetManager {

    object Offsets : Table("event_stream_offsets") {
        val name = varchar("name", 100)
        val lastProcessedOffset = long("last_processed_offset")
        val primaryKeyConstraintConflictTarget = primaryKeyConstraintConflictTarget(name)

        override val primaryKey = PrimaryKey(name)
    }

    override suspend fun getOffset(offsetKey: String) = database.transaction {
        Offsets.slice(lastProcessedOffset).select { name eq offsetKey }
            .map { row -> row[lastProcessedOffset] }.firstOrNull()
    }

    override suspend fun saveOffset(offsetKey: String, offset: Long) {
        database.transaction { tx ->
            val rowsAffected = Offsets.upsert(primaryKeyConstraintConflictTarget, { name eq offsetKey }) {
                it[name] = offsetKey
                it[lastProcessedOffset] = offset
            }
            if (rowsAffected != 1) tx.rollback(UnexpectedNumberOfRowsAffectedInUpdate(rowsAffected, 1))
        }
    }
}
