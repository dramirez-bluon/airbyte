package io.airbyte.cdk.read

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.cdk.consumers.OutputConsumer
import io.airbyte.cdk.jdbc.SourceOperations
import java.util.concurrent.Callable


data class WorkSpec<S : Spec, I : SelectableState<S>>(
    val spec: S,
    val input: I
)

data class WorkResult<S : Spec, I : SelectableState<S>, O : SerializableState<S>>(
    val workSpec: WorkSpec<S, I>,
    val output: O,
    val numRecords: Long
)

data class ThreadSafeWorkerContext(
    val sourceOperations: SourceOperations,
    val outputConsumer: OutputConsumer,
)


sealed interface Worker<
    S : Spec,
    I : SelectableState<S>,
    O : SerializableState<S>
    > : Callable<WorkResult<S,I,O>>{

    val ctx: ThreadSafeWorkerContext
    val workSpec: WorkSpec<S, I>
}


class WorkQueue(val ctx: ThreadSafeWorkerContext) {

    private val q = mutableMapOf<WorkSpec<*,*>, Boolean>()

    fun enqueue(work: Collection<WorkSpec<*,*>>) {
        for (ws in work) {
            q.putIfAbsent(ws, false)
        }
    }

    fun poll(): Worker<*,*,*>? {
        for ((ws, flag) in q) {
            if (!flag) {
                q[ws] = true
                return when (ws.input) {
                    is SelectableGlobalState -> ws.input.worker(ws.spec as GlobalSpec)
                    is SelectableStreamState -> ws.input.worker(ws.spec as StreamSpec)
                }
            }
        }
        return null
    }

    private fun SelectableGlobalState.worker(spec: GlobalSpec)
        : Worker<GlobalSpec, out SelectableGlobalState, out SerializableGlobalState> =
        when (this) {
            is CdcStarting -> CdcWorker(ctx, WorkSpec(spec, this))
            is CdcOngoing -> CdcWorker(ctx, WorkSpec(spec, this))
        }

    private fun SelectableStreamState.worker(spec: StreamSpec)
        : Worker<StreamSpec, out SelectableStreamState, out SerializableStreamState> =
        when (this) {
            is FullRefreshNonResumableStarting ->
                NonResumableFullRefreshWorker(ctx, WorkSpec(spec, this))
            is PrimaryKeyBasedCheckpointing ->
                InitialSyncWorker(ctx, WorkSpec(spec, this))
            is CursorBasedIncrementalOngoing ->
                CursorBasedIncrementalWorker(ctx, WorkSpec(spec, this))
        }
}

class CdcWorker<I>(
    override val ctx: ThreadSafeWorkerContext,
    override val workSpec: WorkSpec<GlobalSpec, I>,
) : Worker<GlobalSpec, I, SerializableGlobalState>
    where I : SelectableGlobalState, I : CdcCheckpointing {

    override fun call(): WorkResult<GlobalSpec, I, SerializableGlobalState> {
        // TODO
        return WorkResult(workSpec, CdcCompleted(workSpec.input.checkpointedCdcValue), 0L)
    }
}

class NonResumableFullRefreshWorker(
    override val ctx: ThreadSafeWorkerContext,
    override val workSpec: WorkSpec<StreamSpec, FullRefreshNonResumableStarting>,
) : Worker<StreamSpec, FullRefreshNonResumableStarting, FullRefreshCompleted> {

    override fun call(): WorkResult<StreamSpec, FullRefreshNonResumableStarting, FullRefreshCompleted> {
        val sql = ctx.sourceOperations.selectFrom(workSpec.spec, workSpec.input)
        // TODO
        return WorkResult(workSpec, FullRefreshCompleted,0L)
    }
}

class InitialSyncWorker<I>(
    override val ctx: ThreadSafeWorkerContext,
    override val workSpec: WorkSpec<StreamSpec, I>,
) : Worker<StreamSpec, I, SerializableStreamState>
    where I : SelectableStreamState, I : PrimaryKeyBasedCheckpointing {
    override fun call(): WorkResult<StreamSpec, I, SerializableStreamState> {
        val sql = ctx.sourceOperations.selectFrom(workSpec.spec, workSpec.input)
        // TODO
        return WorkResult(workSpec, workSpec.input.completed(),0L)
    }
}

class CursorBasedIncrementalWorker(
    override val ctx: ThreadSafeWorkerContext,
    override val workSpec: WorkSpec<StreamSpec, CursorBasedIncrementalOngoing>,
) : Worker<StreamSpec, CursorBasedIncrementalOngoing, SerializableStreamState> {

    override fun call(): WorkResult<StreamSpec, CursorBasedIncrementalOngoing, SerializableStreamState> {
        // TODO
        return WorkResult(workSpec, CursorBasedIncrementalCompleted(workSpec.input.cursor, workSpec.input.checkpointedCursorValue),0L)
    }
}
