/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.operation

import io.airbyte.cdk.command.ConnectorConfigurationSupplier
import io.airbyte.cdk.command.SourceConnectorConfiguration
import io.airbyte.cdk.consumers.OutputConsumer
import io.airbyte.cdk.jdbc.ColumnMetadata
import io.airbyte.cdk.jdbc.ColumnType
import io.airbyte.cdk.jdbc.MetadataQuerier
import io.airbyte.cdk.jdbc.SourceOperations
import io.airbyte.cdk.jdbc.TableName
import io.airbyte.protocol.models.Field
import io.airbyte.protocol.models.v0.AirbyteCatalog
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteStream
import io.airbyte.protocol.models.v0.CatalogHelpers
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger {}

@Singleton
@Requires(property = CONNECTOR_OPERATION, value = "discover")
@Requires(env = ["source"])
class DiscoverOperation(
    val sourceOperations: SourceOperations,
    val metadataQuerier: MetadataQuerier,
    val outputConsumer: OutputConsumer
) : Operation, AutoCloseable {

    override val type = OperationType.DISCOVER

    override fun execute() {
        val airbyteStreams: List<AirbyteStream> = metadataQuerier.tableNames()
            .mapNotNull(::discoveredStream)
            .map {
                // TODO flesh out the catalog with fake CDC columns, etc.
                CatalogHelpers.createAirbyteStream(
                    it.name,
                    it.namespace,
                    it.fields
                )
                    .withSourceDefinedPrimaryKey(it.primaryKeys)
            }
        outputConsumer.accept(AirbyteMessage()
            .withType(AirbyteMessage.Type.CATALOG)
            .withCatalog(AirbyteCatalog().withStreams(airbyteStreams)))
    }

    override fun close() {
        metadataQuerier.close()
    }

    /** Wraps [MetadataQuerier.columnMetadata] with logging and exception handling. */
    private fun discoveredStream(table: TableName): DiscoveredStream? {
        val columnMetadata: List<ColumnMetadata> = metadataQuerier.columnMetadata(table)
        if (columnMetadata.isEmpty()) {
            logger.info { "Skipping empty table $table." }
            return null
        }
        val primaryKeys: List<List<String>> = metadataQuerier.primaryKeys(table)
        val fields: List<Field> = columnMetadata.map { c: ColumnMetadata ->
            Field.of(c.name, sourceOperations.discoverColumnType(c).asJsonSchemaType())
        }
        return DiscoveredStream(
            table.name,
            table.schema ?: table.catalog,
            fields,
            primaryKeys,
        )
    }

    data class DiscoveredStream(
        val name: String,
        val namespace: String?,
        val fields: List<Field>,
        val primaryKeys: List<List<String>>,
    )
}
