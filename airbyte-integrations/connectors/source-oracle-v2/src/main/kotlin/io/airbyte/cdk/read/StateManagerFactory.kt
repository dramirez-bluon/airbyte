package io.airbyte.cdk.read

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.cdk.command.GlobalInputState
import io.airbyte.cdk.command.InputState
import io.airbyte.cdk.command.SourceConnectorConfiguration
import io.airbyte.cdk.command.StreamInputState
import io.airbyte.cdk.command.StreamStateValue
import io.airbyte.cdk.consumers.CatalogValidationFailureHandler
import io.airbyte.cdk.jdbc.CatalogFieldSchema
import io.airbyte.cdk.jdbc.ColumnMetadata
import io.airbyte.cdk.jdbc.ColumnType
import io.airbyte.cdk.jdbc.DiscoverMapper
import io.airbyte.cdk.jdbc.MetadataQuerier
import io.airbyte.cdk.jdbc.TableName
import io.airbyte.commons.exceptions.ConfigErrorException
import io.airbyte.commons.json.Jsons
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.airbyte.protocol.models.v0.AirbyteStream
import io.airbyte.protocol.models.v0.AirbyteStreamNameNamespacePair
import io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog
import io.airbyte.protocol.models.v0.ConfiguredAirbyteStream
import io.airbyte.protocol.models.v0.SyncMode


data class StateManagerFactory(
    val metadataQuerier: MetadataQuerier,
    val discoverMapper: DiscoverMapper,
    val validationHandler: CatalogValidationFailureHandler
) {

    fun create(
        config: SourceConnectorConfiguration,
        configuredCatalog: ConfiguredAirbyteCatalog,
        inputState: InputState,
    ): StateManager {
        val isGlobal: Boolean =
            config.expectedStateType == AirbyteStateMessage.AirbyteStateType.GLOBAL
        when (inputState) {
            is GlobalInputState ->
                if (!isGlobal) {
                    throw ConfigErrorException(
                        "Input state is GLOBAL but config requires ${config.expectedStateType}"
                    )
                }
            is StreamInputState ->
                if (isGlobal) {
                    throw ConfigErrorException(
                        "Input state is STREAM but config requires ${config.expectedStateType}"
                    )
                }
            else -> Unit
        }
        val tableNames: List<TableName> = metadataQuerier.tableNames()
        val streamSpecs: List<StreamSpec> = configuredCatalog.streams.mapNotNull {
            toStreamSpec(it, tableNames)
        }
        val globalSpec = GlobalSpec(streamSpecs)
        return StateManager(
            initialGlobal = when (inputState) {
                is GlobalInputState -> CdcOngoing(globalSpec, inputState.global.cdc)
                else -> if (isGlobal) startCdc(globalSpec) else null
            },
            initialStreams = streamSpecs.map { streamSpec: StreamSpec ->
                val cs: ConfiguredAirbyteStream =
                        configuredCatalog.streams.find { it.stream == streamSpec.stream }!!
                val readKind: ReadKind = when (cs.syncMode) {
                    SyncMode.INCREMENTAL -> if (isGlobal) ReadKind.CDC else ReadKind.CURSOR
                    else -> ReadKind.FULL_REFRESH
                }
                val value: StreamStateValue? =
                        inputState.stream[AirbyteStreamNameNamespacePair.fromConfiguredAirbyteSteam(cs)]
                buildStreamReadState(readKind, streamSpec, value) as SerializableStreamState
            },
        )
    }

    private fun toStreamSpec(configuredStream: ConfiguredAirbyteStream, tableNames: List<TableName>): StreamSpec? {
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

    private fun startCdc(spec: GlobalSpec): State<GlobalSpec> {
        // TODO: add CDC support.
        return CdcStarting(spec, Jsons.emptyObject())
    }

    private fun buildStreamReadState(
        readKind: ReadKind,
        spec: StreamSpec,
        stateValue: StreamStateValue?
    ): State<StreamSpec> {
        if (stateValue == null) {
            return when (readKind) {
                ReadKind.CDC -> startCdcInitialSync(spec)
                ReadKind.CURSOR -> startCursorBasedIncremental(spec)
                ReadKind.FULL_REFRESH -> startFullRefresh(spec)
            }
        }
        val pk: Map<DataColumn, String>? = run {
            if (stateValue.primaryKey.isEmpty()) {
                return@run mapOf()
            }
            val keys: List<DataColumn>? = spec.primaryKeyCandidates.find { pk: List<DataColumn> ->
                pk.map { it.metadata.label }.toSet() == stateValue.primaryKey.keys
            }
            if (keys == null) {
                return@run null
            }
            keys.associateWith { stateValue.primaryKey[it.metadata.label]!! }
        }
        val cursor: Pair<CursorColumn, String>? = run {
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
                    startCdcInitialSync(spec)
                } else if (pk.isNotEmpty()) {
                    CdcInitialSyncOngoing(spec, pk.keys.toList(), pk.values.toList())
                } else {
                    CdcInitialSyncCompleted(spec)
                }
            ReadKind.CURSOR ->
                if (cursor == null || pk == null) {
                    validationHandler.resetStream(spec.name, spec.namespace)
                    startCursorBasedIncremental(spec)
                } else if (pk.isNotEmpty()) {
                    CursorBasedIncrementalInitialSyncOngoing(
                        spec, pk.keys.toList(), pk.values.toList(), cursor.first, cursor.second)
                } else {
                    CursorBasedIncrementalOngoing(spec, cursor.first, cursor.second)
                }
            ReadKind.FULL_REFRESH ->
                if (pk == null) {
                    validationHandler.resetStream(spec.name, spec.namespace)
                    startFullRefresh(spec)
                } else if (pk.isNotEmpty()) {
                    FullRefreshResumableOngoing(spec, pk.keys.toList(), pk.values.toList())
                } else {
                    FullRefreshCompleted(spec)
                }
        }
    }

    private fun startCdcInitialSync(spec: StreamSpec): State<StreamSpec> =
        spec.pickedPrimaryKey
            ?.let { CdcInitialSyncStarting(spec, it) }
            ?: CdcInitialSyncNotStarted(spec)

    private fun startCursorBasedIncremental(spec: StreamSpec): State<StreamSpec> {
        if (spec.pickedCursor == null || spec.pickedPrimaryKey == null) {
            return CursorBasedIncrementalNotStarted(spec)
        }
        val cursorValue: String =
            metadataQuerier.maxCursorValue(spec.table, spec.pickedCursor.name)
                ?: return CursorBasedIncrementalNotStarted(spec)
        return CursorBasedIncrementalInitialSyncStarting(
            spec,
            spec.pickedPrimaryKey,
            spec.pickedCursor,
            cursorValue,
        )
    }

    private fun startFullRefresh(spec: StreamSpec): State<StreamSpec> =
        spec.pickedPrimaryKey
            ?.let { FullRefreshResumableStarting(spec, it) }
            ?: FullRefreshNonResumableStarting(spec)

    private enum class ReadKind { CURSOR, CDC, FULL_REFRESH }
}
