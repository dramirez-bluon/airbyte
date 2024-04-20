package io.airbyte.cdk.read

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.cdk.command.GlobalStateValue
import io.airbyte.cdk.command.StreamStateValue

sealed interface State<S : Spec> {
    val spec: S
}
sealed interface GlobalState : State<GlobalSpec>
sealed interface StreamState : State<StreamSpec>

sealed interface SerializableState<S : Spec> : State<S>
sealed interface SerializableGlobalState : GlobalState, SerializableState<GlobalSpec>
sealed interface SerializableStreamState : StreamState, SerializableState<StreamSpec>

sealed interface SelectableState<S : Spec> : State<S>
sealed interface SelectableGlobalState : GlobalState, SelectableState<GlobalSpec>
sealed interface CdcWorkPending : SelectableGlobalState
sealed interface SelectableStreamState : StreamState, SelectableState<StreamSpec>
sealed interface NonResumableSelectWorkPending : SelectableStreamState
sealed interface ResumableSelectWorkPending : SelectableStreamState


data class CdcNotStarted(
    override val spec: GlobalSpec
) : GlobalState

data class CdcStarting(
    override val spec: GlobalSpec,
    val checkpointedCdcValue: JsonNode
) : GlobalState, CdcWorkPending

data class CdcOngoing(
    override val spec: GlobalSpec,
    val checkpointedCdcValue: JsonNode
) : GlobalState, CdcWorkPending, SerializableGlobalState

data class CdcCompleted(
    override val spec: GlobalSpec,
    val checkpointedCdcValue: JsonNode
) : GlobalState, SerializableGlobalState

data class FullRefreshNotStarted(
    override val spec: StreamSpec,
) : StreamState

data class FullRefreshNonResumableStarting(
    override val spec: StreamSpec,
) : StreamState, NonResumableSelectWorkPending

data class FullRefreshResumableStarting(
    override val spec: StreamSpec,
    val primaryKey: List<DataColumn>
) : StreamState, ResumableSelectWorkPending

data class FullRefreshResumableOngoing(
    override val spec: StreamSpec,
    val primaryKey: List<DataColumn>,
    val checkpointedPrimaryKeyValues: List<String>,
) : StreamState, ResumableSelectWorkPending, SerializableStreamState

data class FullRefreshCompleted(
    override val spec: StreamSpec,
) : StreamState, SerializableStreamState


data class CursorBasedIncrementalNotStarted(
    override val spec: StreamSpec,
) : StreamState

data class CursorBasedIncrementalInitialSyncStarting(
    override val spec: StreamSpec,
    val primaryKey: List<DataColumn>,
    val cursor: CursorColumn,
    val checkpointedCursorValue: String,
) : StreamState, ResumableSelectWorkPending

data class CursorBasedIncrementalInitialSyncOngoing(
    override val spec: StreamSpec,
    val primaryKey: List<DataColumn>,
    val checkpointedPrimaryKeyValues: List<String>,
    val cursor: CursorColumn,
    val checkpointedCursorValue: String,
) : StreamState, ResumableSelectWorkPending, SerializableStreamState

data class CursorBasedIncrementalOngoing(
    override val spec: StreamSpec,
    val cursor: CursorColumn,
    val checkpointedCursorValue: String,
) : StreamState, ResumableSelectWorkPending, SerializableStreamState

data class CursorBasedIncrementalCompleted(
    override val spec: StreamSpec,
    val cursor: CursorColumn,
    val checkpointedCursorValue: String,
) : StreamState, SerializableStreamState

data class CdcInitialSyncNotStarted(
    override val spec: StreamSpec,
) : StreamState

data class CdcInitialSyncStarting(
    override val spec: StreamSpec,
    val primaryKey: List<DataColumn>,
) : StreamState, ResumableSelectWorkPending

data class CdcInitialSyncOngoing(
    override val spec: StreamSpec,
    val primaryKey: List<DataColumn>,
    val checkpointedPrimaryKeyValues: List<String>,
) : StreamState, ResumableSelectWorkPending, SerializableStreamState

data class CdcInitialSyncCompleted(
    override val spec: StreamSpec,
) : StreamState, SerializableStreamState

fun SerializableGlobalState.value(): GlobalStateValue = when (this) {
    is CdcOngoing -> GlobalStateValue(this.checkpointedCdcValue)
    is CdcCompleted -> GlobalStateValue(this.checkpointedCdcValue)
}

fun SerializableStreamState.value(): StreamStateValue = when (this) {
    is FullRefreshResumableOngoing -> StreamStateValue(
        primaryKey.map { it.metadata.label }.zip(checkpointedPrimaryKeyValues).toMap(),
        mapOf())
    is FullRefreshCompleted -> StreamStateValue(mapOf(), mapOf())
    is CursorBasedIncrementalInitialSyncOngoing -> StreamStateValue(
        primaryKey.map { it.metadata.label }.zip(checkpointedPrimaryKeyValues).toMap(),
        mapOf(cursor.name to checkpointedCursorValue))
    is CursorBasedIncrementalOngoing -> StreamStateValue(
        mapOf(),
        mapOf(cursor.name to checkpointedCursorValue))
    is CursorBasedIncrementalCompleted -> StreamStateValue(
        mapOf(),
        mapOf(cursor.name to checkpointedCursorValue))
    is CdcInitialSyncOngoing -> StreamStateValue(
        primaryKey.map { it.metadata.label }.zip(checkpointedPrimaryKeyValues).toMap(),
        mapOf())
    is CdcInitialSyncCompleted -> StreamStateValue(mapOf(), mapOf())
}
