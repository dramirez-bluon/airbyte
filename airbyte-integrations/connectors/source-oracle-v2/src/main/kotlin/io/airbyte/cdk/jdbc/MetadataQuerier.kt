package io.airbyte.cdk.jdbc

import java.util.function.Supplier

/** A very thin abstraction around JDBC metadata queries, to help with testing. */
interface MetadataQuerier : AutoCloseable {

    /**
     * Queries the information_schema for all table names in the schemas specified by the connector
     * configuration.
     */
    fun tableNames(): List<TableName>

    /** Executes a SELECT * on the table, discards the results, and extracts all column metadata. */
    fun columnMetadata(table: TableName): List<ColumnMetadata>

    /** Queries the information_schema for all primary keys for the given table. */
    fun primaryKeys(table: TableName): List<List<String>>

    interface SessionFactory : Supplier<MetadataQuerier> {
        val discoverMapper: DiscoverMapper
    }
}

