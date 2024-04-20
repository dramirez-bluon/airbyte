package io.airbyte.cdk.read

import com.fasterxml.jackson.databind.JsonNode
import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.concurrent.atomic.AtomicBoolean

class NonResumableSelectWorker(
    override val ctx: ThreadSafeWorkerContext,
    override val input: NonResumableSelectWorkPending,
) : Worker<StreamSpec, NonResumableSelectWorkPending, SerializableStreamState> {

    private val logger = KotlinLogging.logger {}

    private val flag = AtomicBoolean()

    override fun signalStop() {
        flag.set(true)
    }

    override fun call(): WorkResult<StreamSpec, NonResumableSelectWorkPending, out SerializableStreamState> {
        val spec: StreamSpec = when (input) {
            is FullRefreshNonResumableStarting -> input.spec
        }
        val sql = ctx.sourceOperations.selectFrom(input)
        // TODO
        return WorkResult(input, FullRefreshCompleted(spec),0L)
    }
}
