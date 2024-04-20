package io.airbyte.cdk.read

import io.airbyte.cdk.consumers.OutputConsumer
import io.airbyte.cdk.jdbc.SourceOperations
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.concurrent.Callable

interface Worker<
    S : Spec,
    I : SelectableState<S>,
    O : SerializableState<S>
    > : Callable<WorkResult<S,I,out O>>{

    val ctx: ThreadSafeWorkerContext
    val input: I
}

data class WorkResult<S : Spec, I : SelectableState<S>, O : SerializableState<S>>(
    val input: I,
    val output: O,
    val numRecords: Long
)

data class ThreadSafeWorkerContext(
    val sourceOperations: SourceOperations,
    val outputConsumer: OutputConsumer,
)

class WorkerRunner(
    private val ctx: ThreadSafeWorkerContext,
    private val stateManager: StateManager,
    private var state: State<out Spec>,
) : Runnable {

    private val logger = KotlinLogging.logger {}

    override fun run() {
        while (true) {
            logger.info { "processing state $state" }
            val worker: Worker<*,*,*> = when (val input: State<out Spec> = state) {
                is CdcStarting ->
                    CdcWorker(ctx, input)
                is CdcOngoing ->
                    CdcWorker(ctx, input)
                is FullRefreshNonResumableStarting ->
                    NonResumableSelectWorker(ctx, input)
                is InitialSync ->
                    ResumableSelectWorker(ctx, input)
                is CursorBasedIncrementalOngoing ->
                    ResumableSelectWorker(ctx, input)
                else -> break
            }
            logger.info { "calling $worker" }
            val workResult: WorkResult<*,*,*> = worker.call()
            logger.info { "${workResult.numRecords} produced by $worker" }
            when (workResult.output) {
                is GlobalState -> stateManager.set(workResult.output, workResult.numRecords)
                is StreamState -> stateManager.set(workResult.output, workResult.numRecords)
            }
            val checkpoint: List<AirbyteStateMessage> = stateManager.checkpoint()
            logger.info { "checkpoint of ${checkpoint.size} state message(s)" }
            checkpoint.forEach(ctx.outputConsumer::accept)
            state = workResult.output
        }
        logger.info { "reached terminal state $state." }
    }

}


