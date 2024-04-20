package io.airbyte.cdk.read

import com.fasterxml.jackson.databind.JsonNode
import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.concurrent.atomic.AtomicBoolean


class CdcWorker(
    override val ctx: ThreadSafeWorkerContext,
    override val input: CdcWorkPending,
) : Worker<GlobalSpec, CdcWorkPending, SerializableGlobalState> {

    private val logger = KotlinLogging.logger {}

    private val flag = AtomicBoolean()

    override fun signalStop() {
        flag.set(true)
    }

    override fun call(): WorkResult<GlobalSpec, CdcWorkPending, out SerializableGlobalState> {
        val cdcValue: JsonNode = when (input) {
            is CdcStarting -> input.checkpointedCdcValue
            is CdcOngoing -> input.checkpointedCdcValue
        }

        // TODO
        return WorkResult(input, CdcCompleted(input.spec, cdcValue), 0L)
    }
}
