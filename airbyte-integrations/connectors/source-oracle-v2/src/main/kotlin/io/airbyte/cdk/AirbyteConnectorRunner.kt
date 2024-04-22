/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk

import io.micronaut.core.cli.CommandLine as MicronautCommandLine
import io.airbyte.cdk.command.ConnectorCommandLinePropertySource
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.configuration.picocli.MicronautFactory
import io.micronaut.context.ApplicationContext
import io.micronaut.context.ApplicationContextBuilder
import io.micronaut.context.env.CommandLinePropertySource
import io.micronaut.context.env.Environment
import picocli.CommandLine

/**
 * Replacement for the Micronaut CLI application runner that configures the CLI components and adds
 * the custom property source used to turn the arguments into configuration properties.
 */
class AirbyteConnectorRunner {

    enum class ConnectorType {
        SOURCE,
        DESTINATION
    }

    companion object {

        @JvmStatic
        fun <R : Runnable> run(
            connectorType: ConnectorType,
            cls: Class<R>,
            vararg args: String,
        ) {
            val commandLine: MicronautCommandLine = MicronautCommandLine.parse(*args)
            val configPropertySource = ConnectorCommandLinePropertySource(commandLine)
            val commandLinePropertySource = CommandLinePropertySource(commandLine)
            val ctxBuilder: ApplicationContextBuilder =
                ApplicationContext.builder(cls, Environment.CLI, connectorType.name.lowercase())
                    .propertySources(configPropertySource, commandLinePropertySource)
            val ctx: ApplicationContext = ctxBuilder.start()
            run(cls, ctx, *args)
        }

        @JvmStatic
        fun <R : Runnable> run(
            cls: Class<R>,
            ctx: ApplicationContext,
            vararg args: String,
        ) {
            CommandLine(cls, MicronautFactory(ctx)).execute(*args)
        }
    }
}
