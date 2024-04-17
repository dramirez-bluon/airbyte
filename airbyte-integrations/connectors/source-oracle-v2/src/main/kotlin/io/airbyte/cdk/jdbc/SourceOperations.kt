package io.airbyte.cdk.jdbc

import io.airbyte.cdk.read.CursorColumn
import io.airbyte.cdk.read.DataColumn
import io.airbyte.cdk.read.SelectableStreamReadState
import io.airbyte.cdk.read.StreamSpec
import java.sql.ResultSet

/** Database-specific query builders and type mappers. */
interface SourceOperations {

    fun selectFrom(
        streamSpec: StreamSpec,
        readState: SelectableStreamReadState,
    ): SqlQueryWithBindings

    data class SqlQueryWithBindings(val sql: String, val params: List<String>)

    fun selectCheckpointLimit(table: TableName): String =
        "SELECT 10000"

    fun toColumnInSelect(c: ColumnMetadata): String =
        c.name

    fun toFullyQualifiedName(table: TableName): String =
        listOfNotNull(
            table.catalog,
            table.schema,
            table.name,
        ).joinToString(separator = ".")

    fun dataColumnValues(streamSpec: StreamSpec, rs: ResultSet): List<Any?> =
        streamSpec.dataColumns.map { c: DataColumn ->
            rs.getObject(c.metadata.name).let { if (rs.wasNull()) null else it }
        }

    fun cursorColumnValue(streamSpec: StreamSpec, rs: ResultSet): String? =
        if (streamSpec.pickedCursor == null) {
            null
        } else {
            rs.getString(streamSpec.pickedCursor.name)?.let { if (rs.wasNull()) null else it }
        }

    fun mapArrayColumnValue(value: Any?): Any? = value

    fun mapLeafColumnValue(leafType: LeafType, value: Any?): Any? = value

}
