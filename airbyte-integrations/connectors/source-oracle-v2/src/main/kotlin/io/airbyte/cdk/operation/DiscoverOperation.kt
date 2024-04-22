/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.operation

import io.airbyte.cdk.consumers.OutputConsumer
import io.airbyte.cdk.jdbc.ColumnMetadata
import io.airbyte.cdk.jdbc.DiscoveredStream
import io.airbyte.cdk.jdbc.MetadataQuerier
import io.airbyte.cdk.jdbc.TableName
import io.airbyte.protocol.models.v0.AirbyteCatalog
import io.airbyte.protocol.models.v0.AirbyteStream
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger {}

@Singleton
@Requires(property = CONNECTOR_OPERATION, value = "discover")
@Requires(env = ["source"])
class DiscoverOperation(
    val metadataQuerierFactory: MetadataQuerier.SessionFactory,
    val outputConsumer: OutputConsumer
) : Operation {

    override val type = OperationType.DISCOVER

    override fun execute() {
        val discoveredStreams: List<DiscoveredStream> =
            metadataQuerierFactory.get().use { metadataQuerier: MetadataQuerier ->
                metadataQuerier.tableNames().mapNotNull { tableName: TableName ->
                    val columnMetadata: List<ColumnMetadata> =
                        metadataQuerier.columnMetadata(tableName)
                    if (columnMetadata.isEmpty()) {
                        logger.info { "Skipping no-column table $tableName." }
                        return@mapNotNull null
                    }
                    val primaryKeys: List<List<String>> = metadataQuerier.primaryKeys(tableName)
                    DiscoveredStream(tableName, columnMetadata, primaryKeys)
                }
            }
        val airbyteStreams: List<AirbyteStream> =
            discoveredStreams.map { metadataQuerierFactory.discoverMapper.airbyteStream(it) }
        outputConsumer.accept(AirbyteCatalog().withStreams(airbyteStreams))
    }
}
