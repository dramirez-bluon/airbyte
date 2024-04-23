package io.airbyte.cdk.read

import io.airbyte.cdk.consumers.OutputConsumer
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.github.oshai.kotlinlogging.KotlinLogging
import java.time.Duration
import java.util.concurrent.Callable
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

interface Worker<S : Spec, I : State<S>> : Callable<WorkResult<S,I>>{

    val input: I

    val spec: S
        get() = input.spec

    fun signalStop()
}

typealias GlobalWorker<T> = Worker<GlobalSpec, T>
typealias StreamWorker<T> = Worker<StreamSpec, T>
interface WorkerFactory {

    fun create(input: CdcNotStarted): GlobalWorker<CdcNotStarted>
    fun create(input: CdcStarting): GlobalWorker<CdcStarting>
    fun create(input: CdcOngoing): GlobalWorker<CdcOngoing>

    fun create(input: CdcInitialSyncNotStarted): StreamWorker<CdcInitialSyncNotStarted>
    fun create(input: CdcInitialSyncStarting): StreamWorker<CdcInitialSyncStarting>
    fun create(input: CdcInitialSyncOngoing): StreamWorker<CdcInitialSyncOngoing>

    fun create(input: FullRefreshNotStarted): StreamWorker<FullRefreshNotStarted>
    fun create(input: FullRefreshResumableStarting): StreamWorker<FullRefreshResumableStarting>
    fun create(input: FullRefreshResumableOngoing): StreamWorker<FullRefreshResumableOngoing>

    fun create(input: CursorBasedNotStarted): StreamWorker<CursorBasedNotStarted>
    fun create(input: CursorBasedInitialSyncStarting): StreamWorker<CursorBasedInitialSyncStarting>
    fun create(input: CursorBasedInitialSyncOngoing): StreamWorker<CursorBasedInitialSyncOngoing>
    fun create(input: CursorBasedIncrementalStarting): StreamWorker<CursorBasedIncrementalStarting>
    fun create(input: CursorBasedIncrementalOngoing): StreamWorker<CursorBasedIncrementalOngoing>

    companion object {
        fun <S : Spec, I : State<S>> shortcut(input: I, output: State<S>): Worker<S, I> =
            object : Worker<S, I> {
                override val input: I = input

                override fun signalStop() {}

                override fun call(): WorkResult<S, I> =
                    WorkResult(input, output, 0L)
            }
    }

}

data class WorkResult<S : Spec, I : State<S>>(
    val input: I,
    val output: State<S>,
    val numRecords: Long = 0L
)

class WorkerThreadRunnable(
    private val factory: WorkerFactory,
    private val workUnitTimeout: Duration,
    private val outputConsumer: OutputConsumer,
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
        logger.info { "$name: new state machine execution" }
        while (true) {
            logger.info { "$name: processing state $state" }
            val worker: Worker<*,*> = when (val input: State<out Spec> = state) {
                is CdcNotStarted -> factory.create(input)
                is CdcStarting -> factory.create(input)
                is CdcOngoing -> factory.create(input)
                is CdcCompleted -> break

                is CdcInitialSyncNotStarted -> factory.create(input)
                is CdcInitialSyncStarting -> factory.create(input)
                is CdcInitialSyncOngoing -> factory.create(input)
                is CdcInitialSyncCompleted -> break

                is FullRefreshNotStarted -> factory.create(input)
                is FullRefreshResumableStarting -> factory.create(input)
                is FullRefreshResumableOngoing -> factory.create(input)
                is FullRefreshCompleted -> break

                is CursorBasedNotStarted -> factory.create(input)
                is CursorBasedInitialSyncStarting -> factory.create(input)
                is CursorBasedInitialSyncOngoing -> factory.create(input)
                is CursorBasedIncrementalStarting -> factory.create(input)
                is CursorBasedIncrementalOngoing -> factory.create(input)
                is CursorBasedIncrementalCompleted -> break
            }
            logger.info { "$name: calling ${worker.javaClass.simpleName}" }
            val future: Future<out WorkResult<*,*>> = ex.submit(worker)
            val result: WorkResult<*,*> = try {
                future.get(workUnitTimeout.toMillis(), TimeUnit.MILLISECONDS)
            } catch (_: TimeoutException) {
                logger.info { "$name: ${worker.javaClass.simpleName} soft timeout" }
                worker.signalStop()
                future.get()
            }
            logger.info { "$name: ${result.numRecords} produced by $worker" }
            state = result.output
            when (result.output) {
                is SerializableGlobalState -> stateManager.set(result.output, result.numRecords)
                is SerializableStreamState -> stateManager.set(result.output, result.numRecords)
                else -> continue
            }
            val checkpoint: List<AirbyteStateMessage> = stateManager.checkpoint()
            logger.info { "$name: checkpoint of ${checkpoint.size} state message(s)" }
            checkpoint.forEach(outputConsumer::accept)
        }
        logger.info { "$name: reached terminal state $state" }
    }

}


