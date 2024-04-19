package io.airbyte.cdk.read

import io.airbyte.cdk.jdbc.ColumnMetadata
import io.airbyte.cdk.jdbc.ColumnType
import io.airbyte.cdk.jdbc.TableName
import io.airbyte.protocol.models.v0.AirbyteStream
import io.airbyte.protocol.models.v0.AirbyteStreamNameNamespacePair
import io.airbyte.protocol.models.v0.ConfiguredAirbyteStream
import io.airbyte.protocol.models.v0.StreamDescriptor
import io.airbyte.protocol.models.v0.SyncMode


sealed interface Spec

data class GlobalSpec(
    val streamSpecs: List<StreamSpec>
) : Spec

data class StreamSpec(
    val configuredStream: ConfiguredAirbyteStream,
    val table: TableName,
    val dataColumns: List<DataColumn>,
    val primaryKeyCandidates: List<List<DataColumn>>,
    val cursorCandidates: List<CursorColumn>,
    val configuredSyncMode: SyncMode,
    val configuredPrimaryKey: List<DataColumn>?,
    val configuredCursor: CursorColumn?,
) : Spec {

    val stream: AirbyteStream = configuredStream.stream

    val name: String = configuredStream.stream.name

    val namespace: String? = configuredStream.stream.namespace

    val namePair: AirbyteStreamNameNamespacePair =
        AirbyteStreamNameNamespacePair.fromConfiguredAirbyteSteam(configuredStream)

    val streamDescriptor: StreamDescriptor =
        StreamDescriptor().withName(name).withNamespace(namespace)

    val pickedPrimaryKey: List<DataColumn>? =
        configuredPrimaryKey ?: primaryKeyCandidates.firstOrNull()

    val pickedCursor: CursorColumn? =
        configuredCursor ?: cursorCandidates.firstOrNull()
}

sealed interface Column {
    val type: ColumnType
}

data class DataColumn(
    val metadata: ColumnMetadata,
    override val type: ColumnType
) : Column

data class CursorColumn(
    val name: String,
    override val type: ColumnType,
) : Column
