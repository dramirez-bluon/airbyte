/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.operation

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.cdk.command.ConfiguredAirbyteCatalogSupplier
import io.airbyte.cdk.command.ConnectorConfigurationSupplier
import io.airbyte.cdk.command.ConnectorInputStateSupplier
import io.airbyte.cdk.command.GlobalInputState
import io.airbyte.cdk.command.GlobalStateValue
import io.airbyte.cdk.command.InputState
import io.airbyte.cdk.command.SourceConnectorConfiguration
import io.airbyte.cdk.command.StreamInputState
import io.airbyte.cdk.command.StreamStateValue
import io.airbyte.cdk.consumers.CatalogValidationFailureHandler
import io.airbyte.cdk.consumers.OutputConsumer
import io.airbyte.cdk.jdbc.CatalogFieldSchema
import io.airbyte.cdk.jdbc.ColumnMetadata
import io.airbyte.cdk.jdbc.ColumnType
import io.airbyte.cdk.jdbc.DiscoverMapper
import io.airbyte.cdk.jdbc.JdbcConnectionFactory
import io.airbyte.cdk.jdbc.MetadataQuerier
import io.airbyte.cdk.jdbc.RowToRecordData
import io.airbyte.cdk.jdbc.SourceOperations
import io.airbyte.cdk.jdbc.TableName
import io.airbyte.commons.exceptions.ConfigErrorException
import io.airbyte.commons.json.Jsons
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteRecordMessage
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.airbyte.protocol.models.v0.AirbyteStateStats
import io.airbyte.protocol.models.v0.AirbyteStream
import io.airbyte.protocol.models.v0.AirbyteStreamNameNamespacePair
import io.airbyte.protocol.models.v0.AirbyteStreamState
import io.airbyte.protocol.models.v0.CatalogHelpers
import io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog
import io.airbyte.protocol.models.v0.ConfiguredAirbyteStream
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.time.Instant


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
) : Operation, AutoCloseable {

    override val type = OperationType.READ

    override fun execute() {
        val selectFroms: List<SelectFrom> = collectSpecs()
        val emittedAt = Instant.now()
        for (selectFrom in selectFroms) {
            val rowToRecordData = RowToRecordData(sourceOperations, selectFrom)
            val conn: Connection = jdbcConnectionFactory.get()
            try {
                conn.isReadOnly = true
                selectFrom.table.catalog?.let { conn.catalog = it }
                selectFrom.table.schema?.let { conn.schema = it }
                val q = sourceOperations.selectFrom(selectFrom)
                val stmt: PreparedStatement = conn.prepareStatement(q.sql)
                var rowCount = 0L
                var cursorValues: List<String?> = selectFrom.cursorColumns.map { it.initialValue }
                try {
                    q.params.forEachIndexed { i, v -> stmt.setString(i+1, v) }
                    val rs: ResultSet = stmt.executeQuery()
                    while (rs.next()) {
                        cursorValues = sourceOperations.cursorColumnValues(selectFrom, rs)
                        val row = sourceOperations.dataColumnValues(selectFrom, rs)
                        val recordData: JsonNode = rowToRecordData.apply(row)
                        outputConsumer.accept(AirbyteMessage()
                            .withType(AirbyteMessage.Type.RECORD)
                            .withRecord(
                                AirbyteRecordMessage()
                                .withStream(selectFrom.streamDescriptor.name)
                                .withNamespace(selectFrom.streamDescriptor.namespace)
                                .withEmittedAt(emittedAt.toEpochMilli())
                                .withData(recordData)))
                        rowCount++
                    }
                    val streamState: JsonNode = Jsons.jsonNode(
                        selectFrom.cursorColumns
                            .map { it.name }
                            .zip(cursorValues)
                            .toMap())
                    outputConsumer.accept(AirbyteMessage()
                        .withType(AirbyteMessage.Type.STATE)
                        .withState(AirbyteStateMessage()
                            .withType(AirbyteStateMessage.AirbyteStateType.STREAM)
                            .withStream(AirbyteStreamState()
                                .withStreamDescriptor(selectFrom.streamDescriptor)
                                .withStreamState(streamState))
                            .withSourceStats(AirbyteStateStats()
                                .withRecordCount(rowCount.toDouble()))))
                } finally {
                    stmt.close()
                }
            } finally {
                conn.close()
            }
        }
    }

    private fun collectSpecs(): ReadFutureSpecs {
        val config: SourceConnectorConfiguration = configSupplier.get()
        val configuredCatalog: ConfiguredAirbyteCatalog = catalogSupplier.get()
        val inputState: InputState = stateSupplier.get()
        when (inputState) {
            is GlobalInputState ->
                if (config.expectedStateType != AirbyteStateMessage.AirbyteStateType.GLOBAL) {
                    throw ConfigErrorException(
                        "Input state is GLOBAL but config requires ${config.expectedStateType}"
                    )
                }
            is StreamInputState ->
                if (config.expectedStateType != AirbyteStateMessage.AirbyteStateType.STREAM) {
                    throw ConfigErrorException(
                        "Input state is STREAM but config requires ${config.expectedStateType}"
                    )
                }
            else -> Unit
        }
        val tableNames: List<TableName> = metadataQuerier.tableNames()
        val streamReadFutureSpecs: List<StreamReadFutureSpec> = configuredCatalog.streams
            .mapNotNull { configuredStream: ConfiguredAirbyteStream ->
                toStreamSpec(configuredStream.stream, tableNames)?.let {
                    val state = inputState.getStateValue(it.stream.name, it.stream.namespace)
                    toStreamReadFutureSpec(configuredStream, it, state)
                }
            }
        if (config.expectedStateType == AirbyteStateMessage.AirbyteStateType.GLOBAL) {
            val globalReadFutureSpec = GlobalReadFutureSpec(
                if (inputState is GlobalInputState) inputState.global else GlobalStateValue(null),
                streamReadFutureSpecs.map { it.streamSpec }
            )
            return ReadFutureSpecs(
                globalReadFutureSpec,
                streamReadFutureSpecs,
            )
        }
        return ReadFutureSpecs(null, streamReadFutureSpecs)
    }

    private fun toStreamSpec(stream: AirbyteStream, tableNames: List<TableName>): StreamSpec? {
        val jsonSchemaProperties: JsonNode = stream.jsonSchema["properties"]
        val name: String = stream.name!!
        val namespace: String? = stream.namespace
        val matchingTables: List<TableName> = tableNames
            .filter { it.name == name }
            .filter { it.catalog == namespace || it.schema == namespace || namespace == null }
        val table: TableName = when (matchingTables.size) {
            0 -> {
                validationHandler.tableNotFound(name, namespace)
                return null
            }
            1 -> matchingTables.first()
            else -> {
                validationHandler.multipleTablesFound(name, namespace, matchingTables)
                return null
            }
        }
        val expectedColumnLabels: Set<String> =
            jsonSchemaProperties.fieldNames().asSequence().toSet()
        val columnMetadata: List<ColumnMetadata> = metadataQuerier.columnMetadata(table)
        val allDataColumns: Map<String, DataColumn> = columnMetadata.associate {
            val schema = CatalogFieldSchema(jsonSchemaProperties[it.name])
            it.label to DataColumn(it, schema.asColumnType())
        }
        for (columnLabel in expectedColumnLabels.toList().sorted()) {
            if (columnLabel.startsWith("_ab_")) {
                // Ignore airbyte metadata columns.
                // These aren't actually present in the table.
                continue
            }
            val column: DataColumn? = allDataColumns[columnLabel]
            if (column == null) {
                validationHandler.columnNotFound(name, namespace, columnLabel)
                continue
            }
            val discoveredType: ColumnType = discoverMapper.columnType(column.metadata)
            if (column.type != discoveredType) {
                validationHandler.columnTypeMismatch(
                    name, namespace, columnLabel, column.type, discoveredType)
                continue
            }
        }
        val streamDataColumns: List<DataColumn> = allDataColumns
            .filterKeys { expectedColumnLabels.contains(it) }
            .values.toList()
        val primaryKeyCandidates: List<List<DataColumn>> = stream.sourceDefinedPrimaryKey
            .map { primaryKeyColumnLabels: List<String> ->
                primaryKeyColumnLabels.map {
                    allDataColumns[it].apply {
                        if (this == null) {
                            validationHandler.columnNotFound(name, namespace, it)
                        }
                    }
                }
            }
            .filter { it.filterNotNull().size == it.size && it.isNotEmpty() }
            .map { it.filterNotNull() }
        val cursorCandidates: List<CursorColumn> = stream.defaultCursorField.mapNotNull {
            val jsonSchema: JsonNode? = jsonSchemaProperties[it]
            if (jsonSchema == null) {
                validationHandler.columnNotFound(name, namespace, it)
                null
            } else {
                CursorColumn(it, CatalogFieldSchema(jsonSchema).asColumnType())
            }
        }
        return StreamSpec(stream, table, streamDataColumns, primaryKeyCandidates, cursorCandidates)
    }

    private fun toStreamReadFutureSpec(
        configuredStream: ConfiguredAirbyteStream,
        streamSpec: StreamSpec,
        inputStateValue: StreamStateValue,
    ): StreamReadFutureSpec {
        TODO()
        return StreamReadFutureSpec(
            streamSpec,
            listOf(),
            null,
            inputStateValue,
        )
    }

    override fun close() {
        metadataQuerier.close()
    }
}


data class ReadFutureSpecs(
    val global: GlobalReadFutureSpec?,
    val streams: List<StreamReadFutureSpec>
) {
    fun toSpecList(): List<ReadFutureSpec> = listOfNotNull(global) + streams
}

sealed interface ReadFutureSpec

data class GlobalReadFutureSpec(
    val global: GlobalStateValue,
    val streamSpecs: List<StreamSpec>
) : ReadFutureSpec

data class StreamReadFutureSpec(
    val streamSpec: StreamSpec,
    val primaryKey: List<DataColumn>?,
    val cursor: CursorColumn?,
    val initialState: StreamStateValue
) : ReadFutureSpec

data class StreamSpec(
    val stream: AirbyteStream,
    val table: TableName,
    val dataColumns: List<DataColumn>,
    val primaryKeyCandidates: List<List<DataColumn>>,
    val cursorColumnCandidates: List<CursorColumn>
)

sealed interface Column {
    val type: ColumnType
}
data class DataColumn(
    val metadata: ColumnMetadata,
    override val type: ColumnType
) : Column
data class CursorColumn(
    val label: String,
    override val type: ColumnType,
) : Column
