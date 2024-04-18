/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.operation

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.cdk.command.ConfiguredAirbyteCatalogSupplier
import io.airbyte.cdk.command.ConnectorConfigurationSupplier
import io.airbyte.cdk.command.ConnectorInputStateSupplier
import io.airbyte.cdk.command.SourceConnectorConfiguration
import io.airbyte.cdk.consumers.CatalogValidationFailureHandler
import io.airbyte.cdk.consumers.OutputConsumer
import io.airbyte.cdk.jdbc.DiscoverMapper
import io.airbyte.cdk.jdbc.JdbcConnectionFactory
import io.airbyte.cdk.jdbc.MetadataQuerier
import io.airbyte.cdk.jdbc.RowToRecordData
import io.airbyte.cdk.jdbc.SourceOperations
import io.airbyte.cdk.read.ReadSpecsToStates
import io.airbyte.cdk.read.StateManagerFactory
import io.airbyte.cdk.read.SelectableStreamState
import io.airbyte.cdk.read.StateManager
import io.airbyte.cdk.read.StreamSpec
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteRecordMessage
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.airbyte.protocol.models.v0.AirbyteStateStats
import io.airbyte.protocol.models.v0.AirbyteStreamState
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.time.Clock
import java.time.Instant

private val logger = KotlinLogging.logger {}

@Singleton
@Requires(property = CONNECTOR_OPERATION, value = "read")
@Requires(env = ["source"])
class ReadOperation(
    val configSupplier: ConnectorConfigurationSupplier<SourceConnectorConfiguration>,
    val catalogSupplier: ConfiguredAirbyteCatalogSupplier,
    val stateSupplier: ConnectorInputStateSupplier,
    //
    val discoverMapper: DiscoverMapper,
    val metadataQuerier: MetadataQuerier,
    //
    val sourceOperations: SourceOperations,
    val jdbcConnectionFactory: JdbcConnectionFactory,
    val outputConsumer: OutputConsumer,
    val validationHandler: CatalogValidationFailureHandler,
    val clock: Clock
) : Operation, AutoCloseable {

    override val type = OperationType.READ

    val emittedAt: Instant = Instant.now(clock)

    private val factory = StateManagerFactory(
        metadataQuerier,
        discoverMapper,
        validationHandler
    )

    override fun execute() {
        val stateManager: StateManager =
            factory.create(configSupplier.get(), catalogSupplier.get(), stateSupplier.get())
        for ((streamSpec, streamReadState) in stateManager.getStreams()) {
            when (streamReadState) {
                is SelectableStreamState -> readStream(streamSpec, streamReadState)
                else -> logger.info { "Skipping ${streamSpec.table} with state $streamReadState." }
            }
        }
    }

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


    override fun close() {
        metadataQuerier.close()
    }
}


