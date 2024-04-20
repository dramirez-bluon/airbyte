package io.airbyte.cdk.read

import io.github.oshai.kotlinlogging.KotlinLogging

class NonResumableSelectWorker<I>(
    override val ctx: ThreadSafeWorkerContext,
    override val input: I,
) : Worker<StreamSpec, I, SerializableStreamState>
    where I : SelectableStreamState, I : NonResumableSelectWorkPending {

    private val logger = KotlinLogging.logger {}

    override fun call(): WorkResult<StreamSpec, I, out SerializableStreamState> {
        val sql = ctx.sourceOperations.selectFrom(input)
        // TODO
        return WorkResult(input, FullRefreshCompleted(input.spec),0L)
    }
}
