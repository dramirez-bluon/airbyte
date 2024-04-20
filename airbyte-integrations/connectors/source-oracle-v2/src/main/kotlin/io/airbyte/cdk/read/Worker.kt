package io.airbyte.cdk.read

import io.airbyte.cdk.consumers.OutputConsumer
import io.airbyte.cdk.jdbc.SourceOperations
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.github.oshai.kotlinlogging.KotlinLogging
import java.time.Duration
import java.util.concurrent.Callable
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

interface Worker<
    S : Spec,
    I : SelectableState<S>,
    O : SerializableState<S>
    > : Callable<WorkResult<S,I,out O>>{

    val ctx: ThreadSafeWorkerContext
    val input: I

    fun signalStop()
}

data class WorkResult<S : Spec, I : SelectableState<S>, O : SerializableState<S>>(
    val input: I,
    val output: O,
    val numRecords: Long
)

data class ThreadSafeWorkerContext(
    val timeout: Duration,
    val sourceOperations: SourceOperations,
    val outputConsumer: OutputConsumer,
)

class WorkerRunner(
    private val ctx: ThreadSafeWorkerContext,
    private val stateManager: StateManager,
    private var state: State<out Spec>,
) : Runnable {

    val name: String = "worker-" + when (val spec = state.spec) {
        is GlobalSpec -> "global"
        is StreamSpec -> spec.namePair.toString()
    }

    private val logger = KotlinLogging.logger {}

    private val ex: ExecutorService = Executors.newSingleThreadExecutor { Thread(it, name) }

    override fun run() {
        while (true) {
            logger.info { "$name: processing state $state" }
            val worker: Worker<*,*,*> = when (val input: State<out Spec> = state) {
                is CdcWorkPending -> CdcWorker(ctx, input)
                is NonResumableSelectWorkPending -> NonResumableSelectWorker(ctx, input)
                is ResumableSelectWorkPending -> ResumableSelectWorker(ctx, input)
                else -> break
            }
            logger.info { "$name: calling ${worker.javaClass.simpleName}" }
            val future: Future<out WorkResult<*,*,*>> = ex.submit(worker)
            val result: WorkResult<*,*,*> = try {
                future.get(ctx.timeout.toMillis(), TimeUnit.MILLISECONDS)
            } catch (_: TimeoutException) {
                logger.info { "$name: ${worker.javaClass.simpleName} soft timeout" }
                worker.signalStop()
                future.get()
            }
            logger.info { "$name: ${result.numRecords} produced by $worker" }
            when (result.output) {
                is GlobalState -> stateManager.set(result.output, result.numRecords)
                is StreamState -> stateManager.set(result.output, result.numRecords)
            }
            val checkpoint: List<AirbyteStateMessage> = stateManager.checkpoint()
            logger.info { "$name: checkpoint of ${checkpoint.size} state message(s)" }
            checkpoint.forEach(ctx.outputConsumer::accept)
            state = result.output
        }
        logger.info { "$name: reached terminal state $state." }
    }

}


