package io.airbyte.cdk.jdbc

import java.sql.ResultSet

/** Database-specific query builders and type mappers. */
interface SourceOperations {

    fun selectStarFromTableLimit0(table: TableName): String
    fun discoverColumnType(c: ColumnMetadata): ColumnType
    fun selectFrom(selectFrom: SelectFrom): SqlQueryWithBindings
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

    fun dataColumnValues(selectFrom: SelectFrom, rs: ResultSet): List<Any?> =
        selectFrom.dataColumns.map { c: DataColumn ->
            rs.getObject(c.metadata.name).let { if (rs.wasNull()) null else it }
        }

    fun cursorColumnValues(selectFrom: SelectFrom, rs: ResultSet): List<String> =
        selectFrom.cursorColumns.map { c: CursorColumn ->
            rs.getString(c.name).let { if (rs.wasNull()) null else it }
                ?: throw RuntimeException("Unexpected NULL value for cursor column ${c.name}.")
        }

    fun mapArrayColumnValue(value: Any?): Any? = value

    fun mapLeafColumnValue(leafType: LeafType, value: Any?): Any? = value

}
