package io.airbyte.integrations.source.oracle

import io.airbyte.cdk.consumers.OutputConsumer
import io.airbyte.cdk.operation.CONNECTOR_OPERATION
import io.airbyte.cdk.operation.ReadOperation
import io.airbyte.cdk.operation.SpecOperation
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
@Property(name = CONNECTOR_OPERATION, value = "read")
class OracleSourceReadTest {

    @Inject lateinit var readOperation: ReadOperation

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
    @Property(name = "airbyte.connector.catalog.json", value = CATALOG)
    internal fun testRead() {
        readOperation.execute()
        results.forEach { println(Jsons.jsonNode(it)) }
    }
}

const val CATALOG = """
{
  "streams": [
    {
      "stream": {
        "name": "KV",
        "json_schema": {
          "type": "object",
          "properties": {
            "V": {
              "type": "string"
            },
            "K": {
              "type": "number"
            }
          }
        },
        "supported_sync_modes": [
          "full_refresh"
        ],
        "default_cursor_field": [],
        "source_defined_primary_key": [
          [
            "K"
          ]
        ],
        "namespace": "FOO"
      },
      "sync_mode": "full_refresh",
      "cursor_field": [],
      "destination_sync_mode": "overwrite",
      "primary_key": []
    }
  ]
}    
"""
