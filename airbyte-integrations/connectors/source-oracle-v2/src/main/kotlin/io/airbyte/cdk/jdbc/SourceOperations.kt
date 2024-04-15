package io.airbyte.cdk.jdbc

import io.airbyte.protocol.models.JsonSchemaType
import java.sql.ResultSet

/** Database-specific query builders and type mappers. */
interface SourceOperations {

    fun selectStarFromTableLimit0(table: TableName): String

    fun selectCheckpointLimit(table: TableName): String =
        "SELECT 10000"

    fun selectFrom(selectFrom: SelectFrom): SqlQueryWithBindings

    fun toAirbyteType(c: ColumnMetadata): JsonSchemaType

    fun toColumnInSelect(c: ColumnMetadata): String =
        c.name

    fun toFullyQualifiedName(table: TableName): String =
        listOfNotNull(
            table.catalog,
            table.schema,
            table.name,
        ).joinToString(separator = ".")

    fun dataColumnValues(selectFrom: SelectFrom, rs: ResultSet): List<Any?> =
        selectFrom.dataColumns.map { c: ColumnMetadata ->
            rs.getObject(c.name).let { if (rs.wasNull()) null else it }
        }

    fun cursorColumnValues(selectFrom: SelectFrom, rs: ResultSet): List<String> =
        selectFrom.cursorColumnNames.map { columnLabel: String ->
            rs.getString(columnLabel)
                .let { if (rs.wasNull()) null else it }
                ?: throw RuntimeException("Unexpected NULL value for cursor column $columnLabel.")
        }

    data class SqlQueryWithBindings(val sql: String, val params: List<String>)

}
