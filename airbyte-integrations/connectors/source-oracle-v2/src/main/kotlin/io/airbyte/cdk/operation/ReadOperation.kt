/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.operation

import io.airbyte.cdk.command.ConnectorConfigurationSupplier
import io.airbyte.cdk.command.SourceConnectorConfiguration
import io.airbyte.cdk.consumers.OutputConsumer
import io.airbyte.cdk.read.StateManager
import io.airbyte.cdk.read.StateManagerSupplier
import io.airbyte.cdk.read.WorkerFactory
import io.airbyte.cdk.read.WorkerThreadRunnable
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.ThreadFactory

private val logger = KotlinLogging.logger {}

@Singleton
@Requires(property = CONNECTOR_OPERATION, value = "read")
@Requires(env = ["source"])
class ReadOperation(
    val stateManagerSupplier: StateManagerSupplier,
    val workerFactory: WorkerFactory,
    val configSupplier: ConnectorConfigurationSupplier<SourceConnectorConfiguration>,
    val outputConsumer: OutputConsumer,
) : Operation {

    override val type = OperationType.READ

    override fun execute() {
        val config: SourceConnectorConfiguration = configSupplier.get()
        val stateManager: StateManager = stateManagerSupplier.get()
        val threadFactory = object : ThreadFactory {
            var n = 1L
            override fun newThread(r: Runnable): Thread = Thread(r, "read-worker-${n++}")
        }
        val ex = Executors.newFixedThreadPool(config.workerConcurrency, threadFactory)
        val runnables = stateManager.currentStates().map {
            WorkerThreadRunnable(
                workerFactory,
                config.workUnitSoftTimeout,
                outputConsumer,
                stateManager,
                it
            )
        }
        val futures: Map<Future<*>, String> = runnables.associate { ex.submit(it) to it.name }
        var n = 0L
        for ((future, name) in futures) {
            try {
                future.get()
            } catch (e: ExecutionException) {
                n++
                logger.error(e.cause ?: e) {
                    "exception thrown by '$name', $n total so far"
                }
            }
        }
        if (n > 0) {
            throw OperationExecutionException("incomplete read due to $n thread failure(s)")
        }
    }
/*
    private fun readStream(streamSpec: StreamSpec, readState: SelectableStreamState) {
        val rowToRecordData = RowToRecordData(sourceOperations, streamSpec)
        val conn: Connection = jdbcConnectionFactory.get()
        try {
            conn.isReadOnly = true
            streamSpec.table.catalog?.let { conn.catalog = it }
            streamSpec.table.schema?.let { conn.schema = it }
            val q = sourceOperations.selectFrom(streamSpec, readState)
            val stmt: PreparedStatement = conn.prepareStatement(q.sql)
            var rowCount = 0L
            var cursorValue: String? = selectFrom.cursorColumns.map { it.initialValue }
            try {
                q.params.forEachIndexed { i, v -> stmt.setString(i+1, v) }
                val rs: ResultSet = stmt.executeQuery()
                while (rs.next()) {
                    cursorValue = sourceOperations.cursorColumnValue(selectFrom, rs)
                    val row = sourceOperations.dataColumnValues(selectFrom, rs)
                    val recordData: JsonNode = rowToRecordData.apply(row)
                    outputConsumer.accept(AirbyteRecordMessage()
                            .withStream(selectFrom.streamDescriptor.name)
                            .withNamespace(selectFrom.streamDescriptor.namespace)
                            .withData(recordData))
                    rowCount++
                }
                outputConsumer.accept(AirbyteStateMessage()
                    .withType(AirbyteStateMessage.AirbyteStateType.STREAM)
                    .withStream(AirbyteStreamState()
                        .withStreamDescriptor(selectFrom.streamDescriptor)
                        .withStreamState(streamState))
                    .withSourceStats(AirbyteStateStats()
                        .withRecordCount(rowCount.toDouble())))
            } finally {
                stmt.close()
            }
        } finally {
            conn.close()
        }

    }
*/
}


