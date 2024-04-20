package io.airbyte.cdk.read

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.cdk.command.GlobalStateValue
import io.airbyte.cdk.command.StreamStateValue

sealed interface State<S : Spec> {
    val spec: S
}
sealed interface GlobalState : State<GlobalSpec>
sealed interface StreamState : State<StreamSpec>

sealed interface Terminal
sealed interface CdcWorkPending : SelectableGlobalState
sealed interface NonResumableSelectWorkPending : SelectableStreamState
sealed interface ResumableSelectWorkPending : SelectableStreamState


sealed interface SerializableState<S : Spec> : State<S>
sealed interface SerializableGlobalState : GlobalState, SerializableState<GlobalSpec>
sealed interface SerializableStreamState : StreamState, SerializableState<StreamSpec>

sealed interface SelectableState<S : Spec> : State<S>
sealed interface SelectableGlobalState : GlobalState, SelectableState<GlobalSpec>
sealed interface SelectableStreamState : StreamState, SelectableState<StreamSpec>

sealed interface CdcCheckpointing : GlobalState {
    val checkpointedCdcValue: JsonNode
}


data class CdcNotStarted(
    override val spec: GlobalSpec
) : GlobalState, Terminal

data class CdcStarting(
    override val spec: GlobalSpec,
    override val checkpointedCdcValue: JsonNode
) : GlobalState, SelectableGlobalState, CdcCheckpointing, CdcWorkPending

data class CdcOngoing(
    override val spec: GlobalSpec,
    override val checkpointedCdcValue: JsonNode
) : GlobalState, SelectableGlobalState, SerializableGlobalState, CdcCheckpointing, CdcWorkPending

data class CdcCompleted(
    override val spec: GlobalSpec,
    override val checkpointedCdcValue: JsonNode
) : GlobalState, SerializableGlobalState, CdcCheckpointing, Terminal

sealed interface InitialSync : SelectableStreamState, ResumableSelectWorkPending {
    val primaryKey: List<DataColumn>
}

sealed interface PrimaryKeyCheckpointing : InitialSync {
    val checkpointedPrimaryKeyValues: List<String>
}

data class FullRefreshNotStarted(
    override val spec: StreamSpec,
) : StreamState, Terminal

data class FullRefreshNonResumableStarting(
    override val spec: StreamSpec,
) : StreamState, SelectableStreamState, NonResumableSelectWorkPending

data class FullRefreshResumableStarting(
    override val spec: StreamSpec,
    override val primaryKey: List<DataColumn>
) : StreamState, SelectableStreamState, InitialSync

data class FullRefreshResumableOngoing(
    override val spec: StreamSpec,
    override val primaryKey: List<DataColumn>,
    override val checkpointedPrimaryKeyValues: List<String>,
) : StreamState, SelectableStreamState, SerializableStreamState, InitialSync, PrimaryKeyCheckpointing

data class FullRefreshCompleted(
    override val spec: StreamSpec,
) : StreamState, SerializableStreamState, Terminal

sealed interface CursorCheckpointing {
    val cursor: CursorColumn
    val checkpointedCursorValue: String
}

data class CursorBasedIncrementalNotStarted(
    override val spec: StreamSpec,
) : StreamState, Terminal

data class CursorBasedIncrementalInitialSyncStarting(
    override val spec: StreamSpec,
    override val primaryKey: List<DataColumn>,
    val cursor: CursorColumn,
    val checkpointedCursorValue: String,
) : StreamState, SelectableStreamState, InitialSync

data class CursorBasedIncrementalInitialSyncOngoing(
    override val spec: StreamSpec,
    override val primaryKey: List<DataColumn>,
    override val checkpointedPrimaryKeyValues: List<String>,
    override val cursor: CursorColumn,
    override val checkpointedCursorValue: String,
) : StreamState, SelectableStreamState, SerializableStreamState, InitialSync, PrimaryKeyCheckpointing, CursorCheckpointing

data class CursorBasedIncrementalOngoing(
    override val spec: StreamSpec,
    override val cursor: CursorColumn,
    override val checkpointedCursorValue: String,
) : StreamState, SelectableStreamState, SerializableStreamState, CursorCheckpointing, ResumableSelectWorkPending

data class CursorBasedIncrementalCompleted(
    override val spec: StreamSpec,
    override val cursor: CursorColumn,
    override val checkpointedCursorValue: String,
) : StreamState, SerializableStreamState, CursorCheckpointing, Terminal

data class CdcInitialSyncNotStarted(
    override val spec: StreamSpec,
) : StreamState, Terminal

data class CdcInitialSyncStarting(
    override val spec: StreamSpec,
    override val primaryKey: List<DataColumn>,
) : StreamState, SelectableStreamState, InitialSync

data class CdcInitialSyncOngoing(
    override val spec: StreamSpec,
    override val primaryKey: List<DataColumn>,
    override val checkpointedPrimaryKeyValues: List<String>,
) : StreamState, SelectableStreamState, SerializableStreamState, InitialSync, PrimaryKeyCheckpointing

data class CdcInitialSyncCompleted(
    override val spec: StreamSpec,
) : StreamState, SerializableStreamState, Terminal

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

fun InitialSync.ongoing(newPrimaryKeyValues: List<String>): SerializableStreamState = when (this) {
    is FullRefreshResumableStarting ->
        FullRefreshResumableOngoing(spec, primaryKey, newPrimaryKeyValues)
    is FullRefreshResumableOngoing ->
        copy(checkpointedPrimaryKeyValues = newPrimaryKeyValues)
    is CdcInitialSyncStarting ->
        CdcInitialSyncOngoing(spec, primaryKey, newPrimaryKeyValues)
    is CdcInitialSyncOngoing ->
        copy(checkpointedPrimaryKeyValues = newPrimaryKeyValues)
    is CursorBasedIncrementalInitialSyncStarting ->
        CursorBasedIncrementalInitialSyncOngoing(spec, primaryKey, newPrimaryKeyValues, cursor, checkpointedCursorValue)
    is CursorBasedIncrementalInitialSyncOngoing ->
        copy(checkpointedPrimaryKeyValues = newPrimaryKeyValues)
}

fun InitialSync.completed(): SerializableStreamState = when (this) {
    is FullRefreshResumableStarting ->
        FullRefreshCompleted(spec)
    is FullRefreshResumableOngoing ->
        FullRefreshCompleted(spec)
    is CdcInitialSyncStarting ->
        CdcInitialSyncCompleted(spec)
    is CdcInitialSyncOngoing ->
        CdcInitialSyncCompleted(spec)
    is CursorBasedIncrementalInitialSyncStarting ->
        CursorBasedIncrementalCompleted(spec, cursor, checkpointedCursorValue)
    is CursorBasedIncrementalInitialSyncOngoing ->
        CursorBasedIncrementalCompleted(spec, cursor, checkpointedCursorValue)
}
