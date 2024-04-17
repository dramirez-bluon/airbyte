package io.airbyte.integrations.source.oracle

import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.cdk.command.ConnectorConfigurationSupplier
import io.airbyte.cdk.command.SourceConnectorConfiguration
import io.airbyte.cdk.jdbc.ColumnMetadata
import io.airbyte.cdk.jdbc.ColumnType
import io.airbyte.cdk.jdbc.DiscoverMapper
import io.airbyte.cdk.jdbc.DiscoveredStream
import io.airbyte.cdk.jdbc.LeafType
import io.airbyte.cdk.jdbc.TableName
import io.airbyte.commons.json.Jsons
import io.airbyte.protocol.models.Field
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.airbyte.protocol.models.v0.AirbyteStream
import io.airbyte.protocol.models.v0.CatalogHelpers
import io.airbyte.protocol.models.v0.SyncMode
import jakarta.inject.Singleton
import java.sql.JDBCType

@Singleton
class OracleSourceDiscoverMapper(
    val configSupplier: ConnectorConfigurationSupplier<OracleSourceConfiguration>
) : DiscoverMapper {

    override fun selectStarFromTableLimit0(table: TableName): String {
        // Oracle doesn't do LIMIT, instead we need to involve ROWNUM.
        return "SELECT * FROM ${table.fullyQualifiedName()} WHERE ROWNUM < 1"
    }

    override fun columnType(c: ColumnMetadata): ColumnType =
        // This is underspecified and almost certainly incorrect! TODO.
        when (c.type) {
            JDBCType.BIT,
            JDBCType.BOOLEAN -> LeafType.BOOLEAN
            JDBCType.TINYINT,
            JDBCType.SMALLINT,
            JDBCType.INTEGER,
            JDBCType.BIGINT -> LeafType.INTEGER
            JDBCType.FLOAT,
            JDBCType.DOUBLE,
            JDBCType.REAL,
            JDBCType.NUMERIC,
            JDBCType.DECIMAL -> LeafType.NUMBER
            JDBCType.CHAR,
            JDBCType.NCHAR,
            JDBCType.NVARCHAR,
            JDBCType.VARCHAR,
            JDBCType.LONGVARCHAR -> LeafType.STRING
            JDBCType.DATE -> LeafType.DATE
            JDBCType.TIME -> LeafType.TIME_WITHOUT_TIMEZONE
            JDBCType.TIMESTAMP -> LeafType.TIMESTAMP_WITHOUT_TIMEZONE
            JDBCType.TIME_WITH_TIMEZONE -> LeafType.TIME_WITH_TIMEZONE
            JDBCType.TIMESTAMP_WITH_TIMEZONE -> LeafType.TIMESTAMP_WITH_TIMEZONE
            JDBCType.BLOB,
            JDBCType.BINARY,
            JDBCType.VARBINARY,
            JDBCType.LONGVARBINARY -> LeafType.BINARY
            JDBCType.ARRAY -> LeafType.STRING
            else -> LeafType.STRING
        }

    override fun isPossibleCursor(c: ColumnMetadata): Boolean =
        when (c.type) {
            JDBCType.TIMESTAMP_WITH_TIMEZONE,
            JDBCType.TIMESTAMP,
            JDBCType.TIME_WITH_TIMEZONE,
            JDBCType.TIME,
            JDBCType.DATE,
            JDBCType.TINYINT,
            JDBCType.SMALLINT,
            JDBCType.INTEGER,
            JDBCType.BIGINT,
            JDBCType.FLOAT,
            JDBCType.DOUBLE,
            JDBCType.REAL,
            JDBCType.NUMERIC,
            JDBCType.DECIMAL,
            JDBCType.NVARCHAR,
            JDBCType.VARCHAR,
            JDBCType.LONGVARCHAR -> true
            else -> false
        }

    override fun selectMaxCursorValue(table: TableName, cursorColumnLabel: String): String =
        "SELECT MAX($cursorColumnLabel) FROM ${table.fullyQualifiedName()}"

    private fun TableName.fullyQualifiedName(): String =
        if (schema == null) name else "${schema}.${name}"

    override fun airbyteStream(stream: DiscoveredStream): AirbyteStream =
        DiscoverMapper.basicAirbyteStream(this, stream).apply {
            namespace = stream.table.schema
            supportedSyncModes = listOf(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL)
            when (configSupplier.get().cursorConfiguration) {
                CdcCursor -> {
                    (jsonSchema["properties"] as ObjectNode).apply {
                        set<ObjectNode>("_ab_cdc_lsn", LeafType.NUMBER.asJsonSchema())
                        set<ObjectNode>("_ab_cdc_updated_at", LeafType.TIMESTAMP_WITH_TIMEZONE.asJsonSchema())
                        set<ObjectNode>("_ab_cdc_deleted_at", LeafType.TIMESTAMP_WITH_TIMEZONE.asJsonSchema())
                    }
                    defaultCursorField = listOf("_ab_cdc_lsn")
                    sourceDefinedCursor = true
                }
                UserDefinedCursor -> {
                    if (defaultCursorField.isEmpty()) {
                        supportedSyncModes = listOf(SyncMode.FULL_REFRESH)
                    }
                }
            }
        }
}
