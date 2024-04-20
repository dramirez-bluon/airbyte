package io.airbyte.cdk.read

import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.concurrent.atomic.AtomicBoolean


class ResumableSelectWorker(
    override val ctx: ThreadSafeWorkerContext,
    override val input: ResumableSelectWorkPending,
) : Worker<StreamSpec, ResumableSelectWorkPending, SerializableStreamState> {

    private val logger = KotlinLogging.logger {}

    private val flag = AtomicBoolean()

    override fun signalStop() {
        flag.set(true)
    }

    override fun call(): WorkResult<StreamSpec, ResumableSelectWorkPending, out SerializableStreamState> {
        // TODO
        return WorkResult(input, CursorBasedIncrementalCompleted(input.spec, input.cursor, input.checkpointedCursorValue),0L)
    }
}
