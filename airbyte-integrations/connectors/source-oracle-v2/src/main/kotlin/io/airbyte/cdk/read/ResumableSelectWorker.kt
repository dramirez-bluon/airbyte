package io.airbyte.cdk.read

import io.github.oshai.kotlinlogging.KotlinLogging


class ResumableSelectWorker<I>(
    override val ctx: ThreadSafeWorkerContext,
    override val input: I,
) : Worker<StreamSpec, I, SerializableStreamState>
    where I : SelectableStreamState, I : ResumableSelectWorkPending {


    private val logger = KotlinLogging.logger {}

    override fun call(): WorkResult<StreamSpec, I, out SerializableStreamState> {
        // TODO
        return WorkResult(input, CursorBasedIncrementalCompleted(input.spec, input.cursor, input.checkpointedCursorValue),0L)
    }
}
