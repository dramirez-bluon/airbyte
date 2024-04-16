/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.oracle

import io.airbyte.cdk.jdbc.ColumnMetadata
import io.airbyte.cdk.jdbc.ColumnType
import io.airbyte.cdk.jdbc.LeafType
import io.airbyte.cdk.jdbc.SelectFrom
import io.airbyte.cdk.jdbc.SourceOperations
import io.airbyte.cdk.jdbc.TableName
import jakarta.inject.Singleton
import java.sql.JDBCType

/**
 * Oracle-specific implementation of [SourceOperations].
 */
@Singleton
class OracleSourceOperations : SourceOperations {

    override fun selectFrom(selectFrom: SelectFrom): SourceOperations.SqlQueryWithBindings {
        val allColumnNames: List<String> =
            selectFrom.cursorColumns.map { it.name } +
                selectFrom.dataColumns.map { toColumnInSelect(it.metadata) }
        val orderBy: String? = (1..selectFrom.cursorColumns.size).toList()
            .takeIf { it.isNotEmpty() }
            ?.joinToString(separator = ", ") { "$it" }
        val (whereClause: String?, allBindings: List<String>) = whereClauseAndBindings(selectFrom)
        val sql: String = listOfNotNull(
            "SELECT ${allColumnNames.joinToString(separator = ", ")}",
            "FROM ${toFullyQualifiedName(selectFrom.table)}",
            whereClause?.let { " WHERE $it" },
            orderBy?.let { " ORDER BY $it" }
        ).joinToString(separator = " ")
        return SourceOperations.SqlQueryWithBindings(sql, allBindings)
    }

    private fun whereClauseAndBindings(selectFrom: SelectFrom): Pair<String?, List<String>> {
        val limitClause: String? = selectFrom.limit?.let { "ROWNUM < ?" }
        val limitBindings: List<String> = listOfNotNull(selectFrom.limit?.toString())
        val cursorValues: List<String> = selectFrom.cursorColumns.mapNotNull { it.initialValue }
        if (cursorValues.isEmpty()) {
            return limitClause to limitBindings
        }
        val cursorNames: List<String> = selectFrom.cursorColumns.map { it.name }
        val cursorClause: String = cursorNames
            .mapIndexed { i, v -> cursorNames.take(i).map { "$it = ?" } + "$v > ?" }
            .map { it.joinToString(separator = " AND ", prefix =  "(", postfix = ")") }
            .joinToString(separator = " OR ", prefix = "(", postfix = ")")
        val cursorBindings: List<String> = cursorValues
            .flatMapIndexed { i, _ -> cursorValues.take(i+1) }
        val whereClause: String = listOfNotNull(cursorClause, limitClause)
            .joinToString(separator = " AND ")
        val allBindings: List<String> = cursorBindings + limitBindings
        return whereClause to allBindings
    }

}
