package io.airbyte.cdk.jdbc

import io.airbyte.cdk.command.ConnectorConfigurationSupplier
import io.airbyte.cdk.command.SourceConnectorConfiguration
import io.airbyte.cdk.read.CursorColumn
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Prototype
import java.sql.Connection
import java.sql.JDBCType
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.sql.SQLException
import java.sql.Statement

private val logger = KotlinLogging.logger {}

/** Default implementation of [MetadataQuerier]. */
@Prototype
private class JdbcMetadataQuerier(
    val discoverMapper: DiscoverMapper,
    val configSupplier: ConnectorConfigurationSupplier<SourceConnectorConfiguration>,
    val jdbcConnectionFactory: JdbcConnectionFactory
) : MetadataQuerier {

    val config: SourceConnectorConfiguration by lazy { configSupplier.get() }

    val connDelegate: Lazy<Connection> = lazy { jdbcConnectionFactory.get() }
    val conn: Connection by connDelegate

    override fun tableNames(): List<TableName> {
        logger.info { "Querying table names for catalog discovery." }
        try {
            val results = mutableListOf<TableName>()
            for (schema in config.schemas) {
                val rs: ResultSet = conn.metaData.getTables(null, schema, null, null)
                while (rs.next()) {
                    val tableName =
                        TableName(
                            catalog = rs.getString("TABLE_CAT"),
                            schema = rs.getString("TABLE_SCHEM"),
                            name = rs.getString("TABLE_NAME"),
                            type = rs.getString("TABLE_TYPE") ?: "",
                        )
                    results.add(tableName)
                }
            }
            logger.info { "Discovered ${results.size} table(s)." }
            return results.sortedBy {
                "${it.catalog ?: ""}.${it.schema ?: ""}.${it.name}.${it.type}"
            }
        } catch (e: Exception) {
            throw RuntimeException("Table name discovery query failed with exception.", e)
        }
    }

    override fun columnMetadata(table: TableName): List<ColumnMetadata> {
        val sql: String = discoverMapper.selectStarFromTableLimit0(table)
        logger.info { "Querying $sql for catalog discovery." }
        try {
            table.catalog?.let { conn.catalog = it }
            table.schema?.let { conn.schema = it }
            conn.createStatement().use { stmt: Statement ->
                stmt.fetchSize = 1
                val meta: ResultSetMetaData = stmt.executeQuery(sql).metaData
                logger.info { "Discovered ${meta.columnCount} columns in $table." }
                return (1..meta.columnCount).map {
                    ColumnMetadata(
                        name = meta.getColumnName(it),
                        label = meta.getColumnLabel(it),
                        type = swallow { meta.getColumnType(it) }?.let { JDBCType.valueOf(it) },
                        typeName = swallow { meta.getColumnTypeName(it) },
                        klazz = swallow { meta.getColumnClassName(it) }?.let { Class.forName(it) },
                        isAutoIncrement = swallow { meta.isAutoIncrement(it) },
                        isCaseSensitive = swallow { meta.isCaseSensitive(it) },
                        isSearchable = swallow { meta.isSearchable(it) },
                        isCurrency = swallow { meta.isCurrency(it) },
                        isNullable =
                        when (swallow { meta.isNullable(it) }) {
                            ResultSetMetaData.columnNoNulls -> false
                            ResultSetMetaData.columnNullable -> true
                            else -> null
                        },
                        isSigned = swallow { meta.isSigned(it) },
                        displaySize = swallow { meta.getColumnDisplaySize(it) },
                        precision = swallow { meta.getPrecision(it) },
                        scale = swallow { meta.getScale(it) },
                    )
                }
            }
        } catch (e: SQLException) {
            throw RuntimeException("Column metadata query failed with exception.", e)
        }
    }

    private fun <T> swallow(supplier: () -> T): T? {
        try {
            return supplier()
        } catch (e: SQLException) {
            logger.debug(e) { "Metadata query triggered exception, ignoring value" }
        }
        return null
    }

    override fun primaryKeys(table: TableName): List<List<String>> {
        logger.info { "Querying primary key metadata for $table for catalog discovery." }
        val pksMap = mutableMapOf<String?, MutableMap<Int, String>>()
        try {
            val rs: ResultSet =
                conn.metaData.getPrimaryKeys(table.catalog, table.schema, table.name)
            while (rs.next()) {
                val pkName: String = rs.getString("PK_NAME") ?: ""
                val pkMap: MutableMap<Int, String> = pksMap.getOrDefault(pkName, mutableMapOf())
                val pkOrdinal: Int = rs.getInt("KEY_SEQ")
                val pkCol: String = rs.getString("COLUMN_NAME")
                pkMap[pkOrdinal] = pkCol
                pksMap[pkName] = pkMap
            }
        } catch (e: SQLException) {
            throw RuntimeException("Primary key metadata query failed with exception.", e)
        }
        logger.info { "Found ${pksMap.size} primary key(s) in $table." }
        return pksMap.toSortedMap(Comparator.naturalOrder<String>()).values.map {
            it.toSortedMap().values.toList()
        }
    }

    override fun maxCursorValue(table: TableName, cursorColumnName: String): String? {
        val sql: String = discoverMapper.selectMaxCursorValue(table, cursorColumnName)
        logger.info { "Querying $sql for sync preparation." }
        try {
            table.catalog?.let { conn.catalog = it }
            table.schema?.let { conn.schema = it }
            conn.createStatement().use { stmt: Statement ->
                stmt.fetchSize = 1
                val rs: ResultSet = stmt.executeQuery(sql)
                val result: String? = if (!rs.next()) {
                    null
                } else {
                    rs.getString(1)?.let { if (rs.wasNull()) null else it }
                }
                logger.info { "Max '$cursorColumnName' value is '${result ?: "NULL"}' in $table." }
                return result
            }
        } catch (e: SQLException) {
            throw RuntimeException("Max cursor value query failed with exception.", e)
        }
    }

    override fun close() {
        if (connDelegate.isInitialized()) {
            logger.info { "Closing JDBC connection." }
            conn.close()
        }
    }
}
