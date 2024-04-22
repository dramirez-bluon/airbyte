package io.airbyte.cdk.read

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.cdk.command.ConfiguredAirbyteCatalogSupplier
import io.airbyte.cdk.command.ConnectorConfigurationSupplier
import io.airbyte.cdk.command.ConnectorInputStateSupplier
import io.airbyte.cdk.command.EmptyInputState
import io.airbyte.cdk.command.GlobalInputState
import io.airbyte.cdk.command.InputState
import io.airbyte.cdk.command.SourceConnectorConfiguration
import io.airbyte.cdk.command.StreamInputState
import io.airbyte.cdk.command.StreamStateValue
import io.airbyte.cdk.consumers.CatalogValidationFailureHandler
import io.airbyte.cdk.jdbc.CatalogFieldSchema
import io.airbyte.cdk.jdbc.ColumnMetadata
import io.airbyte.cdk.jdbc.ColumnType
import io.airbyte.cdk.jdbc.MetadataQuerier
import io.airbyte.cdk.jdbc.TableName
import io.airbyte.commons.exceptions.ConfigErrorException
import io.airbyte.protocol.models.v0.AirbyteStream
import io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog
import io.airbyte.protocol.models.v0.ConfiguredAirbyteStream
import io.airbyte.protocol.models.v0.SyncMode
import jakarta.inject.Singleton
import java.util.function.Supplier


@Singleton
class StateManagerSupplier(
    val configSupplier: ConnectorConfigurationSupplier<SourceConnectorConfiguration>,
    val catalogSupplier: ConfiguredAirbyteCatalogSupplier,
    val stateSupplier: ConnectorInputStateSupplier,
    val metadataQuerierFactory: MetadataQuerier.SessionFactory,
    val validationHandler: CatalogValidationFailureHandler
) : Supplier<StateManager> {

    private val instance: StateManager by lazy {
        val config: SourceConnectorConfiguration = configSupplier.get()
        val configuredCatalog: ConfiguredAirbyteCatalog = catalogSupplier.get()
        val inputState: InputState = stateSupplier.get()
        if (config.global) {
            when (inputState) {
                is GlobalInputState ->
                    createGlobal(configuredCatalog, inputState)
                is StreamInputState ->
                    throw ConfigErrorException("input state unexpectedly of type STREAM")
                is EmptyInputState ->
                    createGlobal(configuredCatalog)
            }
        } else {
            when (inputState) {
                is GlobalInputState ->
                    throw ConfigErrorException("input state unexpectedly of type GLOBAL")
                is StreamInputState ->
                    createStream(configuredCatalog, inputState)
                is EmptyInputState ->
                    createStream(configuredCatalog)
            }
        }
    }
    override fun get(): StateManager = instance

    private fun createGlobal(
        configuredCatalog: ConfiguredAirbyteCatalog,
        globalInputState: GlobalInputState? = null,
    ): StateManager {
        val allStreamSpecs: List<StreamSpec> = catalogStreamSpecs(configuredCatalog)
        val globalStreamSpecs: List<StreamSpec> =
            allStreamSpecs.filter { it.configuredSyncMode == SyncMode.INCREMENTAL }
        return StateManager(
            initialGlobal = if (globalInputState == null) {
                CdcNotStarted(GlobalSpec(globalStreamSpecs))
            } else {
                CdcStarting(GlobalSpec(globalStreamSpecs), globalInputState.global.cdc)
            },
            initialStreams = allStreamSpecs.map { streamSpec: StreamSpec ->
                when (streamSpec.configuredSyncMode) {
                    SyncMode.INCREMENTAL -> buildStreamReadState(
                        ReadKind.CDC,
                        streamSpec,
                        globalInputState?.globalStreams?.get(streamSpec.namePair)
                    )
                    SyncMode.FULL_REFRESH -> buildStreamReadState(
                        ReadKind.FULL_REFRESH,
                        streamSpec,
                        globalInputState?.nonGlobalStreams?.get(streamSpec.namePair)
                    )
                }
            }
        )
    }

    private fun createStream(
        configuredCatalog: ConfiguredAirbyteCatalog,
        streamInputState: StreamInputState? = null,
    ): StateManager {
        return StateManager(
            initialGlobal = null,
            initialStreams = catalogStreamSpecs(configuredCatalog).map { streamSpec: StreamSpec ->
                val readKind: ReadKind = when (streamSpec.configuredSyncMode) {
                    SyncMode.INCREMENTAL -> ReadKind.CURSOR
                    SyncMode.FULL_REFRESH -> ReadKind.FULL_REFRESH
                }
                val stateValue: StreamStateValue? =
                    streamInputState?.streams?.get(streamSpec.namePair)
                buildStreamReadState(readKind, streamSpec, stateValue)
            }
        )
    }

    private fun catalogStreamSpecs(configuredCatalog: ConfiguredAirbyteCatalog): List<StreamSpec> {
        metadataQuerierFactory.get().use { metadataQuerier: MetadataQuerier ->
            val tableNames: List<TableName> = metadataQuerier.tableNames()
            return configuredCatalog.streams.mapNotNull {
                toStreamSpec(metadataQuerier, it, tableNames)
            }
        }
    }

    private fun toStreamSpec(
        metadataQuerier: MetadataQuerier,
        configuredStream: ConfiguredAirbyteStream,
        tableNames: List<TableName>,
    ): StreamSpec? {
        val stream: AirbyteStream = configuredStream.stream
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
            val discoveredType: ColumnType =
                metadataQuerierFactory.discoverMapper.columnType(column.metadata)
            if (column.type != discoveredType) {
                validationHandler.columnTypeMismatch(
                    name, namespace, columnLabel, column.type, discoveredType)
                continue
            }
        }
        val streamDataColumns: List<DataColumn> = allDataColumns
            .filterKeys { expectedColumnLabels.contains(it) }
            .values.toList()
        fun pkOrNull(pkColumnLabels: List<String>): List<DataColumn>? = pkColumnLabels
            .mapNotNull {
                allDataColumns[it].apply {
                    if (this == null) validationHandler.columnNotFound(name, namespace, it)
                }
            }
            .takeIf { it.isEmpty() || it.size < pkColumnLabels.size }
        fun cursorOrNull(cursorColumnName: String): CursorColumn? {
            val jsonSchema: JsonNode? = jsonSchemaProperties[cursorColumnName]
            if (jsonSchema == null) {
                validationHandler.columnNotFound(name, namespace, cursorColumnName)
                return null
            }
            return CursorColumn(cursorColumnName, CatalogFieldSchema(jsonSchema).asColumnType())
        }
        val primaryKeyCandidates: List<List<DataColumn>> =
            stream.sourceDefinedPrimaryKey.mapNotNull(::pkOrNull)
        val cursorCandidates: List<CursorColumn> =
            stream.defaultCursorField.mapNotNull(::cursorOrNull)
        val configuredSyncMode: SyncMode = configuredStream.syncMode ?: SyncMode.FULL_REFRESH
        val configuredPrimaryKey: List<DataColumn>? =
            configuredStream.primaryKey?.asSequence()?.mapNotNull(::pkOrNull)?.firstOrNull()
        val configuredCursor: CursorColumn? =
            configuredStream.cursorField?.asSequence()?.mapNotNull(::cursorOrNull)?.firstOrNull()
        return StreamSpec(
            configuredStream,
            table,
            streamDataColumns,
            primaryKeyCandidates,
            cursorCandidates,
            configuredSyncMode,
            configuredPrimaryKey,
            configuredCursor,
        )
    }

    private fun buildStreamReadState(
        readKind: ReadKind,
        spec: StreamSpec,
        stateValue: StreamStateValue?
    ): StreamState {
        val pk: Map<DataColumn, String>? = run {
            if (stateValue == null) {
                return@run null
            }
            if (stateValue.primaryKey.isEmpty()) {
                return@run mapOf()
            }
            val pkKeys: Set<String> = stateValue.primaryKey.keys
            val keys: List<DataColumn>? = spec.primaryKeyCandidates.find { pk: List<DataColumn> ->
                pk.map { it.metadata.label }.toSet() == stateValue.primaryKey.keys
            }
            if (keys == null) {
                validationHandler.invalidPrimaryKey(spec.name, spec.namespace, pkKeys.toList())
                return@run null
            }
            keys.associateWith { stateValue.primaryKey[it.metadata.label]!! }
        }
        val cursor: Pair<CursorColumn, String>? = run {
            if (stateValue == null) {
                return@run null
            }
            if (readKind != ReadKind.CURSOR) {
                return@run null
            }
            val cursorKeys: Set<String> = stateValue.cursors.keys
            if (cursorKeys.size > 1) {
                validationHandler.invalidCursor(spec.name, spec.namespace, cursorKeys.toString())
                return@run null
            }
            val cursorLabel: String = cursorKeys.firstOrNull() ?: return@run null
            val cursorColumn: CursorColumn? = spec.cursorCandidates
                .find { it.name == cursorKeys.first() }
            if (cursorColumn == null) {
                validationHandler.invalidCursor(spec.name, spec.namespace, cursorLabel)
                return@run null
            }
            cursorColumn to stateValue.cursors[cursorLabel]!!
        }
        return when (readKind) {
            ReadKind.CDC ->
                if (pk == null) {
                    validationHandler.resetStream(spec.name, spec.namespace)
                    CdcInitialSyncNotStarted(spec)
                } else if (pk.isNotEmpty()) {
                    CdcInitialSyncOngoing(spec, pk.keys.toList(), pk.values.toList())
                } else {
                    CdcInitialSyncCompleted(spec)
                }
            ReadKind.CURSOR ->
                if (cursor == null || pk == null) {
                    validationHandler.resetStream(spec.name, spec.namespace)
                    CursorBasedNotStarted(spec)
                } else if (pk.isNotEmpty()) {
                    CursorBasedInitialSyncOngoing(
                        spec, pk.keys.toList(), pk.values.toList(), cursor.first, cursor.second)
                } else {
                    CursorBasedIncrementalStarting(spec, cursor.first, cursor.second)
                }
            ReadKind.FULL_REFRESH ->
                if (pk.isNullOrEmpty()) {
                    validationHandler.resetStream(spec.name, spec.namespace)
                    FullRefreshNotStarted(spec)
                } else {
                    FullRefreshResumableOngoing(spec, pk.keys.toList(), pk.values.toList())
                }
        }
    }

    private enum class ReadKind { CURSOR, CDC, FULL_REFRESH }
}
