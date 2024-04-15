package io.airbyte.cdk.jdbc

import io.airbyte.protocol.models.v0.ConfiguredAirbyteStream
import java.sql.JDBCType


/** Models a row for [java.sql.DatabaseMetaData.getTables]. */
data class TableName(
    val catalog: String? = null,
    val schema: String? = null,
    val name: String,
    val type: String,
)

/** Data class with one field for each [java.sql.ResultSetMetaData] column method. */
data class ColumnMetadata(
    val name: String,
    val type: JDBCType? = null,
    val typeName: String? = null,
    val klazz: Class<*>? = null,
    val isAutoIncrement: Boolean? = null,
    val isCaseSensitive: Boolean? = null,
    val isSearchable: Boolean? = null,
    val isCurrency: Boolean? = null,
    val isNullable: Boolean? = null,
    val isSigned: Boolean? = null,
    val displaySize: Int? = null,
    val precision: Int? = null,
    val scale: Int? = null,
)

data class SelectFrom(
    val configuredStream: ConfiguredAirbyteStream,
    val table: TableName,
    val dataColumns: List<ColumnMetadata>,
    val cursorColumnNames: List<String>,
    val cursorColumnValues: List<String>,
    val limit: Long?
)
