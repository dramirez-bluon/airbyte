package io.airbyte.cdk.read

import io.github.oshai.kotlinlogging.KotlinLogging


class CdcWorker<I>(
    override val ctx: ThreadSafeWorkerContext,
    override val input: I,
) : Worker<GlobalSpec, I, SerializableGlobalState>
    where I : SelectableGlobalState, I : CdcWorkPending, I : CdcCheckpointing {

    private val logger = KotlinLogging.logger {}

    override fun call(): WorkResult<GlobalSpec, I, out SerializableGlobalState> {
        // TODO
        return WorkResult(input, CdcCompleted(input.spec, input.checkpointedCdcValue), 0L)
    }
}
