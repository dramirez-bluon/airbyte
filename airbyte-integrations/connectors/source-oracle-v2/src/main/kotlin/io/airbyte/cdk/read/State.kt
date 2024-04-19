package io.airbyte.cdk.read

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.cdk.command.GlobalStateValue
import io.airbyte.cdk.command.StreamStateValue

sealed interface State<S : Spec>
sealed interface GlobalState : State<GlobalSpec>
sealed interface StreamState : State<StreamSpec>


sealed interface SerializableState<S : Spec> : State<S>
sealed interface SerializableGlobalState : SerializableState<GlobalSpec>
sealed interface SerializableStreamState : SerializableState<StreamSpec>

sealed interface SelectableState<S : Spec> : State<S>
sealed interface SelectableGlobalState : SelectableState<GlobalSpec>
sealed interface SelectableStreamState : SelectableState<StreamSpec>

sealed interface CdcCheckpointing : SelectableGlobalState {
    val checkpointedCdcValue: JsonNode
}

data object CdcNotStarted
    : GlobalState

data class CdcStarting(
    override val checkpointedCdcValue: JsonNode
) : GlobalState, CdcCheckpointing

data class CdcOngoing(
    override val checkpointedCdcValue: JsonNode
) : GlobalState, SelectableGlobalState, SerializableGlobalState, CdcCheckpointing

data class CdcCompleted(
    val checkpointedCdcValue: JsonNode
) : GlobalState, SerializableGlobalState

sealed interface PrimaryKeyBasedCheckpointing : StreamState {
    val primaryKey: List<DataColumn>
}

data object FullRefreshNotStarted
    : StreamState

data object FullRefreshNonResumableStarting
    : StreamState, SelectableStreamState

data class FullRefreshResumableStarting(
    override val primaryKey: List<DataColumn>
) : StreamState, SelectableStreamState, PrimaryKeyBasedCheckpointing

data class FullRefreshResumableOngoing(
    override val primaryKey: List<DataColumn>,
    val checkpointedPrimaryKeyValues: List<String>,
) : StreamState, SelectableStreamState, SerializableStreamState, PrimaryKeyBasedCheckpointing

data object FullRefreshCompleted
    : StreamState, SerializableStreamState

data object CursorBasedIncrementalNotStarted
    : StreamState

data class CursorBasedIncrementalInitialSyncStarting(
    override val primaryKey: List<DataColumn>,
    val cursor: CursorColumn,
    val initialCursorValue: String,
) : StreamState, SelectableStreamState, PrimaryKeyBasedCheckpointing

data class CursorBasedIncrementalInitialSyncOngoing(
    override val primaryKey: List<DataColumn>,
    val checkpointedPrimaryKeyValues: List<String>,
    val cursor: CursorColumn,
    val initialCursorValue: String,
) : StreamState, SelectableStreamState, SerializableStreamState, PrimaryKeyBasedCheckpointing

data class CursorBasedIncrementalOngoing(
    val cursor: CursorColumn,
    val checkpointedCursorValue: String,
) : StreamState, SelectableStreamState, SerializableStreamState

data class CursorBasedIncrementalCompleted(
    val cursor: CursorColumn,
    val checkpointedCursorValue: String,
) : StreamState, SerializableStreamState

data object CdcInitialSyncNotStarted
    : StreamState

data class CdcInitialSyncStarting(
    override val primaryKey: List<DataColumn>,
) : StreamState, SelectableStreamState, PrimaryKeyBasedCheckpointing

data class CdcInitialSyncOngoing(
    override val primaryKey: List<DataColumn>,
    val checkpointedPrimaryKeyValues: List<String>,
) : StreamState, SelectableStreamState, SerializableStreamState, PrimaryKeyBasedCheckpointing

data object CdcInitialSyncCompleted
    : StreamState, SerializableStreamState

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
        mapOf(cursor.name to initialCursorValue))
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

fun PrimaryKeyBasedCheckpointing.ongoing(newPrimaryKeyValues: List<String>): SerializableStreamState = when (this) {
    is FullRefreshResumableStarting ->
        FullRefreshResumableOngoing(primaryKey, newPrimaryKeyValues)
    is FullRefreshResumableOngoing ->
        copy(checkpointedPrimaryKeyValues = newPrimaryKeyValues)
    is CdcInitialSyncStarting ->
        CdcInitialSyncOngoing(primaryKey, newPrimaryKeyValues)
    is CdcInitialSyncOngoing ->
        copy(checkpointedPrimaryKeyValues = newPrimaryKeyValues)
    is CursorBasedIncrementalInitialSyncStarting ->
        CursorBasedIncrementalInitialSyncOngoing(primaryKey, newPrimaryKeyValues, cursor, initialCursorValue)
    is CursorBasedIncrementalInitialSyncOngoing ->
        copy(checkpointedPrimaryKeyValues = newPrimaryKeyValues)
}

fun PrimaryKeyBasedCheckpointing.completed(): SerializableStreamState = when (this) {
    is FullRefreshResumableStarting ->
        FullRefreshCompleted
    is FullRefreshResumableOngoing ->
        FullRefreshCompleted
    is CdcInitialSyncStarting ->
        CdcInitialSyncCompleted
    is CdcInitialSyncOngoing ->
        CdcInitialSyncCompleted
    is CursorBasedIncrementalInitialSyncStarting ->
        CursorBasedIncrementalCompleted(cursor, initialCursorValue)
    is CursorBasedIncrementalInitialSyncOngoing ->
        CursorBasedIncrementalCompleted(cursor, initialCursorValue)
}
