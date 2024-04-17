package io.airbyte.cdk.read

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.cdk.command.GlobalStateValue

sealed interface GlobalReadState

sealed interface SerializableGlobalReadState : GlobalReadState

data object CdcNotStarted : GlobalReadState

data class CdcStarting(val checkpointedCdcValue: JsonNode) : GlobalReadState

data class CdcOngoing(val checkpointedCdcValue: JsonNode) : SerializableGlobalReadState

fun SerializableGlobalReadState.value(): GlobalStateValue = when (this) {
    is CdcOngoing -> GlobalStateValue(this.checkpointedCdcValue)
}
