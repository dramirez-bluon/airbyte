/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.operation

import io.airbyte.cdk.integrations.base.AirbyteTraceMessageUtility
import io.airbyte.cdk.integrations.util.ApmTraceUtils
import io.airbyte.cdk.integrations.util.ConnectorExceptionUtil
import io.github.oshai.kotlinlogging.KotlinLogging

private val logger = KotlinLogging.logger {}

const val CONNECTOR_OPERATION: String = "airbyte.connector.operation"

/**
 * Interface that defines a CLI operation.
 * Each operation maps to one of the available [OperationType]s.
 */
interface Operation {

    val type: OperationType

    fun execute()

    fun executeWithExceptionHandling() {
        logger.info { "Executing $type operation." }
        try {
            execute()
        } catch (e: Throwable) {
            // Many of the exceptions thrown are nested inside layers of RuntimeExceptions. An
            // attempt is made to find the root exception that corresponds to a configuration
            // error. If that does not exist, we just return the original exception.
            ApmTraceUtils.addExceptionToTrace(e)
            val rootThrowable = ConnectorExceptionUtil.getRootConfigError(Exception(e))
            val displayMessage = ConnectorExceptionUtil.getDisplayMessage(rootThrowable)
            // If the connector throws a config error, a trace message with the relevant
            // message should be surfaced.
            if (ConnectorExceptionUtil.isConfigError(rootThrowable)) {
                AirbyteTraceMessageUtility.emitConfigErrorTrace(e, displayMessage)
            }
            logger.error(e) { "Failed $type operation execution." }
        }
    }
}

/**
 * Defines the operations that may be invoked via the CLI arguments.
 * Not all connectors will implement all of these operations.
 */
enum class OperationType {
    SPEC,
    CHECK,
    DISCOVER,
    READ,
    WRITE,
}

/** Custom exception that represents a failure to execute an operation. */
class OperationExecutionException(message: String, cause: Throwable) : Exception(message, cause) {
    constructor(message: String) : this(message, RuntimeException(message))

}
