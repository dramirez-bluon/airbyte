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


data class CdcNotStarted(
    override val spec: GlobalSpec
) : GlobalState

data class CdcStarting(
    override val spec: GlobalSpec,
    val checkpointedCdcValue: JsonNode,
) : GlobalState

data class CdcOngoing(
    override val spec: GlobalSpec,
    val checkpointedCdcValue: JsonNode,
    val targetCdcValue: JsonNode,
) : GlobalState, SerializableGlobalState

data class CdcCompleted(
    override val spec: GlobalSpec,
    val checkpointedCdcValue: JsonNode
) : GlobalState, SerializableGlobalState

data class FullRefreshNotStarted(
    override val spec: StreamSpec,
) : StreamState

data class FullRefreshResumableStarting(
    override val spec: StreamSpec,
    val primaryKey: List<DataColumn>
) : StreamState

data class FullRefreshResumableOngoing(
    override val spec: StreamSpec,
    val primaryKey: List<DataColumn>,
    val checkpointedPrimaryKeyValues: List<String>,
) : StreamState, SerializableStreamState

data class FullRefreshCompleted(
    override val spec: StreamSpec,
) : StreamState, SerializableStreamState

data class CursorBasedNotStarted(
    override val spec: StreamSpec,
) : StreamState

data class CursorBasedInitialSyncStarting(
    override val spec: StreamSpec,
    val primaryKey: List<DataColumn>,
    val cursor: CursorColumn,
    val checkpointedCursorValue: String,
) : StreamState

data class CursorBasedInitialSyncOngoing(
    override val spec: StreamSpec,
    val primaryKey: List<DataColumn>,
    val checkpointedPrimaryKeyValues: List<String>,
    val cursor: CursorColumn,
    val checkpointedCursorValue: String,
) : StreamState, SerializableStreamState

data class CursorBasedIncrementalStarting(
    override val spec: StreamSpec,
    val cursor: CursorColumn,
    val checkpointedCursorValue: String,
) : StreamState

data class CursorBasedIncrementalOngoing(
    override val spec: StreamSpec,
    val cursor: CursorColumn,
    val checkpointedCursorValue: String,
    val targetCursorValue: String,
    ) : StreamState, SerializableStreamState

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
) : StreamState

data class CdcInitialSyncOngoing(
    override val spec: StreamSpec,
    val primaryKey: List<DataColumn>,
    val checkpointedPrimaryKeyValues: List<String>,
) : StreamState, SerializableStreamState

data class CdcInitialSyncCompleted(
    override val spec: StreamSpec,
) : StreamState, SerializableStreamState

fun SerializableGlobalState.toGlobalStateValue(): GlobalStateValue = when (this) {
    is CdcOngoing -> GlobalStateValue(this.checkpointedCdcValue)
    is CdcCompleted -> GlobalStateValue(this.checkpointedCdcValue)
}

fun SerializableStreamState.toStreamStateValue(): StreamStateValue = when (this) {
    is FullRefreshResumableOngoing -> StreamStateValue(
        primaryKey.map { it.metadata.label }.zip(checkpointedPrimaryKeyValues).toMap())
    is FullRefreshCompleted -> StreamStateValue()
    is CursorBasedInitialSyncOngoing -> StreamStateValue(
        primaryKey = primaryKey.map { it.metadata.label }.zip(checkpointedPrimaryKeyValues).toMap(),
        cursors = mapOf(cursor.name to checkpointedCursorValue))
    is CursorBasedIncrementalOngoing -> StreamStateValue(
        cursors = mapOf(cursor.name to checkpointedCursorValue))
    is CursorBasedIncrementalCompleted -> StreamStateValue(
        cursors = mapOf(cursor.name to checkpointedCursorValue))
    is CdcInitialSyncOngoing -> StreamStateValue(
        primaryKey.map { it.metadata.label }.zip(checkpointedPrimaryKeyValues).toMap())
    is CdcInitialSyncCompleted -> StreamStateValue()
}
