/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.oracle

import io.airbyte.cdk.jdbc.ColumnMetadata
import io.airbyte.cdk.jdbc.SelectFrom
import io.airbyte.cdk.jdbc.SourceOperations
import io.airbyte.cdk.jdbc.TableName
import io.airbyte.protocol.models.JsonSchemaType
import jakarta.inject.Singleton
import java.sql.JDBCType

/**
 * Oracle-specific implementation of [SourceOperations].
 */
@Singleton
class OracleSourceOperations : SourceOperations {

    override fun selectStarFromTableLimit0(table: TableName) =
        // Oracle doesn't do LIMIT, instead we need to involve ROWNUM.
        "SELECT * FROM ${toFullyQualifiedName(table)} WHERE ROWNUM < 1"

    override fun selectFrom(selectFrom: SelectFrom): SourceOperations.SqlQueryWithBindings {
        val cursors: List<String> = selectFrom.cursorColumnNames
        val dataColumnNames: List<String> = selectFrom.dataColumns.map(::toColumnInSelect)
        val allColumnNames: String = (cursors + dataColumnNames)
            .joinToString(separator = ", ")
        val cursorClause: String? = cursors
            .mapIndexed { i, v -> cursors.take(i).map { "$it = ?" } + "$v > ?" }
            .map { it.joinToString(separator = " AND ", prefix =  "(", postfix = ")") }
            .takeIf { it.isNotEmpty() }
            ?.joinToString(separator = " OR ", prefix = "(", postfix = ")")
        val limitClause: String? = selectFrom.limit?.let { "ROWNUM < ?" }
        val whereClause: String? = listOfNotNull(cursorClause, limitClause)
            .takeIf { it.isNotEmpty() }
            ?.joinToString(separator = " AND ")
        val orderBy: String? = (1..cursors.size).toList()
            .takeIf { it.isNotEmpty() }
            ?.joinToString(separator = ", ") { "$it" }
        val sql: String = listOfNotNull(
            "SELECT $allColumnNames",
            "FROM ${toFullyQualifiedName(selectFrom.table)}",
            whereClause?.let { " WHERE $it" },
            orderBy?.let { " ORDER BY $it" }
        ).joinToString(separator = " ")
        val cursorBindings: List<String> = selectFrom.cursorColumnValues
            .flatMapIndexed { i, _ -> selectFrom.cursorColumnValues.take(i+1) }
        val limitBindings: List<String> = listOfNotNull(selectFrom.limit?.toString())
        val allBindings: List<String> = cursorBindings + limitBindings
        return SourceOperations.SqlQueryWithBindings(sql, allBindings)
    }

    override fun toAirbyteType(c: ColumnMetadata): JsonSchemaType =
        // This is underspecified and almost certainly incorrect! TODO.
        when (c.type) {
            JDBCType.BIT,
            JDBCType.BOOLEAN -> JsonSchemaType.BOOLEAN
            JDBCType.TINYINT,
            JDBCType.SMALLINT -> JsonSchemaType.INTEGER
            JDBCType.INTEGER -> JsonSchemaType.INTEGER
            JDBCType.BIGINT -> JsonSchemaType.INTEGER
            JDBCType.FLOAT,
            JDBCType.DOUBLE -> JsonSchemaType.NUMBER
            JDBCType.REAL -> JsonSchemaType.NUMBER
            JDBCType.NUMERIC,
            JDBCType.DECIMAL -> JsonSchemaType.NUMBER
            JDBCType.CHAR,
            JDBCType.NCHAR,
            JDBCType.NVARCHAR,
            JDBCType.VARCHAR,
            JDBCType.LONGVARCHAR -> JsonSchemaType.STRING
            JDBCType.DATE -> JsonSchemaType.STRING_DATE
            JDBCType.TIME -> JsonSchemaType.STRING_TIME_WITHOUT_TIMEZONE
            JDBCType.TIMESTAMP -> JsonSchemaType.STRING_TIMESTAMP_WITHOUT_TIMEZONE
            JDBCType.TIME_WITH_TIMEZONE -> JsonSchemaType.STRING_TIME_WITH_TIMEZONE
            JDBCType.TIMESTAMP_WITH_TIMEZONE -> JsonSchemaType.STRING_TIMESTAMP_WITH_TIMEZONE
            JDBCType.BLOB,
            JDBCType.BINARY,
            JDBCType.VARBINARY,
            JDBCType.LONGVARBINARY -> JsonSchemaType.STRING_BASE_64
            JDBCType.ARRAY -> JsonSchemaType.ARRAY
            else -> JsonSchemaType.STRING
        }
}
