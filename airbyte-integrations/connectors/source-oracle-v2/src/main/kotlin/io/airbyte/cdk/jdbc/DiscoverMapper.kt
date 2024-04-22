package io.airbyte.cdk.jdbc

import io.airbyte.protocol.models.Field
import io.airbyte.protocol.models.v0.AirbyteStream
import io.airbyte.protocol.models.v0.CatalogHelpers

interface DiscoverMapper {
    fun selectStarFromTableLimit0(table: TableName): String
    fun columnType(c: ColumnMetadata): ColumnType
    fun isPossibleCursor(c: ColumnMetadata): Boolean
    fun airbyteStream(stream: DiscoveredStream): AirbyteStream


    companion object {
        fun basicAirbyteStream(mapper: DiscoverMapper, stream: DiscoveredStream): AirbyteStream {
            val fields: List<Field> = stream.columnMetadata
                .map { Field.of(it.label, mapper.columnType(it).asJsonSchemaType()) }
            val airbyteStream: AirbyteStream = CatalogHelpers.createAirbyteStream(
                stream.table.name,
                null, // Don't know how to map namespace yet, fill in later
                fields
            )
            val nameToLabel: Map<String, String> =
                stream.columnMetadata.associate { it.name to it.label }
            val pkColumnNames: List<List<String>> = stream.primaryKeyColumnNames
                .map { pk: List<String> -> pk.map { nameToLabel[it]!! } }
            airbyteStream
                .withSourceDefinedPrimaryKey(pkColumnNames)
            val cursorColumnLabels: List<String> = stream.columnMetadata
                .filter { mapper.isPossibleCursor(it) }
                .map { it.label }
            airbyteStream
                .withDefaultCursorField(cursorColumnLabels)
            return airbyteStream
        }
    }
}

data class DiscoveredStream(
    val table: TableName,
    val columnMetadata: List<ColumnMetadata>,
    val primaryKeyColumnNames: List<List<String>>
)
