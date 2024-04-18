package io.airbyte.cdk.read

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.cdk.command.GlobalStateValue
import io.airbyte.cdk.command.StreamStateValue

sealed interface State<S : Spec>
sealed interface SerializableState<S : Spec> : State<S>
sealed interface SerializableGlobalState : SerializableState<GlobalSpec>

data object CdcNotStarted : State<GlobalSpec>

data class CdcStarting(val checkpointedCdcValue: JsonNode) : State<GlobalSpec>

data class CdcOngoing(val checkpointedCdcValue: JsonNode) : SerializableGlobalState

fun SerializableGlobalState.value(): GlobalStateValue = when (this) {
    is CdcOngoing -> GlobalStateValue(this.checkpointedCdcValue)
}

sealed interface SelectableStreamState : State<StreamSpec>

sealed interface SerializableStreamState : SerializableState<StreamSpec>

data object FullRefreshNotStarted : State<StreamSpec>

data object FullRefreshNonResumableStarting : SelectableStreamState

data class FullRefreshResumableStarting(val primaryKey: List<DataColumn>) : SelectableStreamState

data class FullRefreshResumableOngoing(
    val primaryKey: List<DataColumn>,
    val checkpointedPrimaryKeyValues: List<String>,
) : SerializableStreamState, SelectableStreamState

data object FullRefreshCompleted : SerializableStreamState

data object CursorBasedIncrementalNotStarted : State<StreamSpec>

data class CursorBasedIncrementalInitialSyncStarting(
    val primaryKey: List<DataColumn>,
    val cursor: CursorColumn,
    val initialCursorValue: String?,
) : SelectableStreamState

data class CursorBasedIncrementalInitialSyncOngoing(
    val primaryKey: List<DataColumn>,
    val checkpointedPrimaryKeyValues: List<String>,
    val cursor: CursorColumn,
    val initialCursorValue: String?,
) : SerializableStreamState, SelectableStreamState

data class CursorBasedIncrementalOngoing(
    val cursor: CursorColumn,
    val checkpointedCursorValue: String?,
) : SerializableStreamState, SelectableStreamState

data object CdcInitialSyncNotStarted : State<StreamSpec>

data class CdcInitialSyncStarting(
    val primaryKey: List<DataColumn>,
) : SelectableStreamState

data class CdcInitialSyncOngoing(
    val primaryKey: List<DataColumn>,
    val checkpointedPrimaryKeyValues: List<String>,
) : SerializableStreamState, SelectableStreamState

data object CdcInitialSyncCompleted : SerializableStreamState

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
    is CdcInitialSyncOngoing -> StreamStateValue(
        primaryKey.map { it.metadata.label }.zip(checkpointedPrimaryKeyValues).toMap(),
        mapOf())
    is CdcInitialSyncCompleted -> StreamStateValue(mapOf(), mapOf())
}
