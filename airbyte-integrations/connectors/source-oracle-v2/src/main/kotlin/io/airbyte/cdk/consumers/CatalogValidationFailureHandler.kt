package io.airbyte.cdk.consumers

import io.airbyte.cdk.jdbc.ColumnType
import io.airbyte.cdk.jdbc.TableName
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton


private val logger = KotlinLogging.logger {}

interface CatalogValidationFailureHandler {

    fun tableNotFound(streamName: String, streamNamespace: String?)

    fun multipleTablesFound(streamName: String, streamNamespace: String?, matches: List<TableName>)

    fun columnNotFound(streamName: String, streamNamespace: String?, columnName: String)

    fun columnTypeMismatch(streamName: String, streamNamespace: String?, columnName: String, expected: ColumnType, actual: ColumnType)

    fun invalidCursor(streamName: String, streamNamespace: String?, cursor: String)

    fun resetStream(streamName: String, streamNamespace: String?)

}

@Singleton
class LoggingCatalogValidationFailureHandler : CatalogValidationFailureHandler {

    override fun tableNotFound(streamName: String, streamNamespace: String?) {
        logger.warn {
            "No matching table found for name '$streamName' in ${inNamespace(streamNamespace)}."
        }
    }

    override fun multipleTablesFound(
        streamName: String,
        streamNamespace: String?,
        matches: List<TableName>
    ) {
        logger.warn {
            "Multiple matching tables found for name '$streamName' in" +
                " ${inNamespace(streamNamespace)}: $matches"
        }
    }

    override fun columnNotFound(streamName: String, streamNamespace: String?, columnName: String) {
        logger.warn {
            "In table '$streamName' in ${inNamespace(streamNamespace)}: " +
                "column '$columnName' not found."
        }
    }

    override fun invalidCursor(streamName: String, streamNamespace: String?, cursor: String) {
        logger.warn {
            "In table '$streamName' in ${inNamespace(streamNamespace)}: " +
                "invalid cursor '$cursor'."
        }
    }

    override fun resetStream(streamName: String, streamNamespace: String?) {
        logger.warn {
            "Resetting stream '$streamName' in ${inNamespace(streamNamespace)}."
        }
    }

    override fun columnTypeMismatch(
        streamName: String,
        streamNamespace: String?,
        columnName: String,
        expected: ColumnType,
        actual: ColumnType
    ) {
        logger.warn {
            "In table '$streamName' in ${inNamespace(streamNamespace)}: " +
                "column '$columnName' has type $actual but configured catalog expects $expected."
        }
    }

    private fun inNamespace(streamNamespace: String?): String =
        if (streamNamespace == null) {
            "unspecified namespace"
        } else {
            "namespace '$streamNamespace'"
        }
}
