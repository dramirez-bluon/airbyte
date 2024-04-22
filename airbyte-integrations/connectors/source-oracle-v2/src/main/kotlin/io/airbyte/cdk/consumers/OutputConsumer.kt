package io.airbyte.cdk.consumers

import io.airbyte.commons.json.Jsons
import io.airbyte.protocol.models.v0.AirbyteCatalog
import io.airbyte.protocol.models.v0.AirbyteConnectionStatus
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteRecordMessage
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.airbyte.protocol.models.v0.AirbyteTraceMessage
import io.airbyte.protocol.models.v0.ConnectorSpecification
import jakarta.inject.Singleton
import java.time.Clock
import java.time.Instant
import java.util.function.Consumer

/** Emits the [AirbyteMessage] instances produced by the connector. */
interface OutputConsumer : Consumer<AirbyteMessage> {

    val emittedAt: Instant

    fun accept(record: AirbyteRecordMessage) {
        record.emittedAt = emittedAt.toEpochMilli()
        accept(AirbyteMessage().withType(AirbyteMessage.Type.RECORD).withRecord(record))
    }

    fun accept(state: AirbyteStateMessage) {
        accept(AirbyteMessage().withType(AirbyteMessage.Type.STATE).withState(state))
    }

    fun accept(spec: ConnectorSpecification) {
        accept(AirbyteMessage().withType(AirbyteMessage.Type.SPEC).withSpec(spec))
    }

    fun accept(status: AirbyteConnectionStatus) {
        accept(AirbyteMessage().withType(AirbyteMessage.Type.SPEC).withConnectionStatus(status))
    }

    fun accept(catalog: AirbyteCatalog) {
        accept(AirbyteMessage().withType(AirbyteMessage.Type.CATALOG).withCatalog(catalog))
    }

    fun accept(trace: AirbyteTraceMessage) {
        accept(AirbyteMessage().withType(AirbyteMessage.Type.TRACE).withTrace(trace))
    }

}

/** Default implementation of [OutputConsumer]. */
@Singleton
class StdoutOutputConsumer(
    clock: Clock
) : OutputConsumer {

    override val emittedAt: Instant = Instant.now(clock)

    override fun accept(airbyteMessage: AirbyteMessage) {
        val json: String = Jsons.serialize(airbyteMessage)
        synchronized(this) {
            println(json)
        }
    }
}
