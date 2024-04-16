package io.airbyte.integrations.source.oracle

import io.airbyte.cdk.consumers.OutputConsumer
import io.airbyte.cdk.operation.CONNECTOR_OPERATION
import io.airbyte.cdk.operation.DiscoverOperation
import io.airbyte.cdk.operation.ReadOperation
import io.airbyte.commons.json.Jsons
import io.airbyte.protocol.models.v0.AirbyteCatalog
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.CatalogHelpers
import io.micronaut.context.annotation.Property
import io.micronaut.test.annotation.MockBean
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Test

@MicronautTest(environments = ["source"])
@Property(name = CONNECTOR_OPERATION, value = "discover")
class OracleSourceDiscoverTest {


    @Inject lateinit var discoverOperation: DiscoverOperation

    var results: MutableList<AirbyteMessage> = mutableListOf()

    @MockBean(OutputConsumer::class)
    fun outputConsumer(): OutputConsumer {
        results = mutableListOf()
        return OutputConsumer {
            results.add(it)
        }
    }

    @Test
    @Property(name = "airbyte.connector.config.host", value = "localhost")
    @Property(name = "airbyte.connector.config.port", value = "1521")
    @Property(name = "airbyte.connector.config.username", value = "FOO")
    @Property(name = "airbyte.connector.config.password", value = "BAR")
    @Property(name = "airbyte.connector.config.schemas", value = "FOO")
    @Property(
        name = "airbyte.connector.config.connection_data.connection_type",
        value = "service_name"
    )
    @Property(name = "airbyte.connector.config.connection_data.service_name", value = "FREEPDB1")
    internal fun testDiscover() {
        discoverOperation.execute()
        val airbyteCatalog: AirbyteCatalog = results.first().catalog
        val configuredCatalog = CatalogHelpers.toDefaultConfiguredCatalog(airbyteCatalog)
        println(Jsons.jsonNode(configuredCatalog))
    }
}
