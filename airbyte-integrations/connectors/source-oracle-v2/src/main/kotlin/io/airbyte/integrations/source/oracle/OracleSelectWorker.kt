package io.airbyte.integrations.source.oracle

import io.airbyte.cdk.consumers.OutputConsumer
import io.airbyte.cdk.jdbc.JdbcConnectionFactory
import io.airbyte.cdk.read.CdcInitialSyncNotStarted
import io.airbyte.cdk.read.CdcInitialSyncOngoing
import io.airbyte.cdk.read.CdcInitialSyncStarting
import io.airbyte.cdk.read.CursorBasedIncrementalOngoing
import io.airbyte.cdk.read.CursorBasedIncrementalStarting
import io.airbyte.cdk.read.CursorBasedInitialSyncOngoing
import io.airbyte.cdk.read.CursorBasedInitialSyncStarting
import io.airbyte.cdk.read.CursorBasedNotStarted
import io.airbyte.cdk.read.FullRefreshNotStarted
import io.airbyte.cdk.read.FullRefreshResumableOngoing
import io.airbyte.cdk.read.FullRefreshResumableStarting
import io.airbyte.cdk.read.StreamSpec
import io.airbyte.cdk.read.StreamState
import io.airbyte.cdk.read.WorkResult
import io.airbyte.cdk.read.Worker
import io.airbyte.cdk.read.WorkerFactory
import io.airbyte.commons.exceptions.ConfigErrorException
import io.airbyte.protocol.models.v0.AirbyteRecordMessage
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Statement
import java.util.concurrent.atomic.AtomicBoolean

class OracleSelectWorker(
    override val input: StreamState,
    val jdbcConnectionFactory: JdbcConnectionFactory,
    val outputConsumer: OutputConsumer,
) : Worker<StreamSpec, StreamState> {

    private val stopRequested = AtomicBoolean()

    override fun signalStop() {
        stopRequested.set(true)
    }

    override fun call(): WorkResult<StreamSpec, StreamState> {
        val pk = spec.pickedPrimaryKey
        when (input) {
            is FullRefreshNotStarted -> {
                if (pk != null) {
                    return shortcut(FullRefreshResumableStarting(spec, pk))
                } else {
                    nonResumableFullRefresh()
                }
            }
            is FullRefreshResumableStarting -> {

            }
            is FullRefreshResumableOngoing -> {

            }
            is CdcInitialSyncNotStarted -> {
                if (pk == null) {
                    throw ConfigErrorException("no known primary key for ${spec.namePair}")
                }
                return shortcut(CdcInitialSyncStarting(spec, pk))
            }
            is CdcInitialSyncStarting -> {

            }
            is CdcInitialSyncOngoing -> {

            }
            is CursorBasedNotStarted -> {
                val cursor = spec.pickedCursor
                    ?: throw ConfigErrorException("no known cursor for ${spec.namePair}")
                if (pk == null) {
                    throw ConfigErrorException("no known primary key for ${spec.namePair}")
                }
                withPreparedStatement("SELECT MAX(${cursor.name}) FROM ${spec.name}") {
                    it.executeQuery().use { rs: ResultSet ->
                        if (!rs.next()) {
                            throw RuntimeException("cursor '${cursor.name}' has no values in ${spec.name}")
                        }
                        val checkpointed: String = rs.getString(1)
                            ?.takeUnless { rs.wasNull() }
                            ?: throw RuntimeException("cursor '${cursor.name}' has NULL values in ${spec.name}")
                        shortcut(CursorBasedInitialSyncStarting(spec, pk, cursor, checkpointed))
                    }
                }
            }
            is CursorBasedInitialSyncStarting -> {

            }
            is CursorBasedInitialSyncOngoing -> {

            }
            is CursorBasedIncrementalStarting -> {

            }
            is CursorBasedIncrementalOngoing -> {

            }
        }

    }

    fun shortcut(output: StreamState): WorkResult<StreamSpec, StreamState> =
        WorkResult(input, output)

    fun nonResumableFullRefresh(): WorkResult<StreamSpec, StreamState> {
        val columnClause = spec.dataColumns.map { it.metadata.name }.joinToString()
        val sql = "SELECT $columnClause FROM ${spec.name}"
        withPreparedStatement(sql) {
            it.executeQuery().use { rs: ResultSet ->
                var numRows = 0L
                while (rs.next()) {

                    spec.dataColumns.mapIndexed { idx, c ->
                        rs.getObject(idx+1)?.takeUnless { rs.wasNull() }
                    }
                        ...
                    outputConsumer.accept(AirbyteRecordMessage()
                        .withStream(spec.name)
                        .withNamespace(spec.namespace)
                        .withData(TODO()))
                    numRows++
                }
            }
        }
    }

    fun query(
        sql: String,
        setBindings: (PreparedStatement) -> Unit,
        rowFn: (ResultSet) -> Unit
    ): {
        val conn: Connection = jdbcConnectionFactory.get()
        try {
            conn.isReadOnly = true
            input.spec.table.catalog?.let { conn.catalog = it }
            input.spec.table.schema?.let { conn.schema = it }
            val stmt: PreparedStatement = conn.prepareStatement(sql)
            var rowCount = 0L
            try {
                setBindings(stmt)
                val rs: ResultSet = stmt.executeQuery()
                while (rs.next()) {


                    cursorValue = sourceOperations.cursorColumnValue(selectFrom, rs)
                    val row = sourceOperations.dataColumnValues(selectFrom, rs)
                    val recordData: JsonNode = rowToRecordData.apply(row)
                    outputConsumer.accept(
                        AirbyteRecordMessage()
                            .withStream(input.spec.name)
                            .withNamespace(input.spec.namespace)
                            .withData(recordData))
                    rowCount++
                }
            } finally {
                stmt.close()
            }
        } finally {
            conn.close()
        }
    }

    fun <T> withPreparedStatement(sql: String, fn: (PreparedStatement) -> T): T {
        val conn: Connection = jdbcConnectionFactory.get()
        try {
            conn.isReadOnly = true
            input.spec.table.catalog?.let { conn.catalog = it }
            input.spec.table.schema?.let { conn.schema = it }
            val stmt: PreparedStatement = conn.prepareStatement(sql)
            try {
                return fn(stmt)
            } finally {
                stmt.close()
            }
        } finally {
            conn.close()
        }
    }



}
