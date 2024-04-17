package io.airbyte.cdk.read

import io.airbyte.cdk.command.StreamStateValue

sealed interface StreamReadState

sealed interface SelectableStreamReadState : StreamReadState

sealed interface SerializableStreamReadState : StreamReadState

data object FullRefreshNotStarted : StreamReadState

data object FullRefreshNonResumableStarting : StreamReadState, SelectableStreamReadState

data class FullRefreshResumableStarting(val primaryKey: List<DataColumn>) : StreamReadState, SelectableStreamReadState

data class FullRefreshResumableOngoing(
    val primaryKey: List<DataColumn>,
    val checkpointedPrimaryKeyValues: List<String>,
) : SerializableStreamReadState, SelectableStreamReadState

data object FullRefreshCompleted : SerializableStreamReadState

data object CursorBasedIncrementalNotStarted : StreamReadState

data class CursorBasedIncrementalInitialSyncStarting(
    val primaryKey: List<DataColumn>,
    val cursor: CursorColumn,
    val initialCursorValue: String?,
) : StreamReadState, SelectableStreamReadState

data class CursorBasedIncrementalInitialSyncOngoing(
    val primaryKey: List<DataColumn>,
    val checkpointedPrimaryKeyValues: List<String>,
    val cursor: CursorColumn,
    val initialCursorValue: String?,
) : SerializableStreamReadState, SelectableStreamReadState

data class CursorBasedIncrementalOngoing(
    val cursor: CursorColumn,
    val checkpointedCursorValue: String?,
) : SerializableStreamReadState, SelectableStreamReadState

data object CdcInitialSyncNotStarted : StreamReadState

data class CdcInitialSyncStarting(
    val primaryKey: List<DataColumn>,
) : StreamReadState, SelectableStreamReadState

data class CdcInitialSyncOngoing(
    val primaryKey: List<DataColumn>,
    val checkpointedPrimaryKeyValues: List<String>,
) : SerializableStreamReadState, SelectableStreamReadState

data object CdcInitialSyncCompleted : SerializableStreamReadState

fun SerializableStreamReadState.value(): StreamStateValue = when (this) {
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
