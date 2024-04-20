package io.airbyte.cdk.read

import io.airbyte.cdk.command.StreamStateValue
import io.airbyte.commons.json.Jsons
import io.airbyte.protocol.models.v0.AirbyteGlobalState
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.airbyte.protocol.models.v0.AirbyteStateStats
import io.airbyte.protocol.models.v0.AirbyteStreamNameNamespacePair
import io.airbyte.protocol.models.v0.AirbyteStreamState

private typealias StreamKey = AirbyteStreamNameNamespacePair

class StateManager(
    initialGlobal: State<GlobalSpec>?,
    initialStreams: Collection<State<StreamSpec>>,
)  {

    private val global: GlobalStateManager?
    private val nonGlobal: Map<StreamKey, NonGlobalStreamStateManager>

    init {
        val streamMap: Map<StreamKey, State<StreamSpec>> =
            initialStreams.associateBy { it.spec.namePair }
        if (initialGlobal == null) {
            global = null
            nonGlobal = streamMap.mapValues { NonGlobalStreamStateManager(it.value) }
        } else {
            val globalStreams: Map<StreamKey, State<StreamSpec>> = initialGlobal.spec.streamSpecs
                .mapNotNull { streamMap[it.namePair] }
                .associateBy { it.spec.namePair }
            global = GlobalStateManager(
                initialGlobalState = initialGlobal,
                initialStreamStates = globalStreams.values)
            nonGlobal = streamMap
                .filterKeys { !globalStreams.containsKey(it) }
                .mapValues { NonGlobalStreamStateManager(it.value) }
        }
    }

    fun get(): List<State<out Spec>> =
        listOfNotNull(global?.state()) +
            (global?.streamStateManagers?.values ?: listOf()).map { it.state() } +
            nonGlobal.values.map { it.state() }

    fun set(state: GlobalState, numRecords: Long) {
        global?.set(state, numRecords)
    }

    fun set(state: StreamState, numRecords: Long) {
        global?.streamStateManagers?.get(state.spec.namePair)?.set(state, numRecords)
        nonGlobal[state.spec.namePair]?.set(state, numRecords)
    }

    fun checkpoint(): List<AirbyteStateMessage> =
        listOfNotNull(global?.checkpoint()) + nonGlobal.mapNotNull {it.value.checkpoint() }

    private sealed class BaseStateManager<S : Spec>(
        initialState: State<S>,
        private val isCheckpointUnique: Boolean = true
    ) {

        val spec: S = initialState.spec

        private var current: State<S> = initialState
        private var pending: State<S> = initialState
        private var pendingNumRecords: Long = 0L

        fun state(): State<S> =
            synchronized(this) { current }

        fun set(state: State<S>, numRecords: Long) {
            synchronized(this) {
                pending = state
                pendingNumRecords += numRecords
            }
        }

        protected fun swap(): Pair<SerializableState<S>, Long>? {
            synchronized(this) {
                if (isCheckpointUnique && pendingNumRecords == 0L && pending == current) {
                    return null
                }
                return when (val pendingState: State<S> = pending) {
                    is SerializableState<S> -> (pendingState to pendingNumRecords).also {
                        current = pendingState
                        pendingNumRecords = 0L
                    }
                    else -> null
                }
            }
        }
    }

    private class GlobalStateManager(
        initialGlobalState: State<GlobalSpec>,
        initialStreamStates: Collection<State<StreamSpec>>
    ) : BaseStateManager<GlobalSpec>(initialGlobalState) {

        val streamStateManagers: Map<StreamKey, GlobalStreamStateManager> =
            initialStreamStates.associate { it.spec.namePair to  GlobalStreamStateManager(it) }

        fun checkpoint(): AirbyteStateMessage? {
            val (state: SerializableState<GlobalSpec>, numRecords: Long) = swap() ?: return null
            var totalNumRecords: Long = numRecords
            val streamStates = mutableListOf<AirbyteStreamState>()
            for ((_, streamStateManager) in streamStateManagers) {
                val (streamState, streamNumRecords) = streamStateManager.checkpointGlobalStream()
                streamStates.add(streamState)
                totalNumRecords += streamNumRecords
            }
            val airbyteGlobalState = AirbyteGlobalState()
                .withSharedState(Jsons.jsonNode((state as SerializableGlobalState).value()))
                .withStreamStates(streamStates)
            return AirbyteStateMessage()
                .withType(AirbyteStateMessage.AirbyteStateType.GLOBAL)
                .withGlobal(airbyteGlobalState)
                .withSourceStats(AirbyteStateStats().withRecordCount(totalNumRecords.toDouble()))
        }
    }

    private class GlobalStreamStateManager(
        initialState: State<StreamSpec>
    ) : BaseStateManager<StreamSpec>(initialState, isCheckpointUnique = false) {

        fun checkpointGlobalStream(): Pair<AirbyteStreamState, Long> {
            val (state: SerializableState<StreamSpec>?, numRecords: Long) = swap() ?: (null to 0L)
            val value: StreamStateValue? = (state as? SerializableStreamState)?.value()
            return AirbyteStreamState()
                .withStreamDescriptor(spec.streamDescriptor)
                .withStreamState(value?.let { Jsons.jsonNode(it) }) to numRecords
        }
    }


    private class NonGlobalStreamStateManager(
        initialState: State<StreamSpec>
    ) : BaseStateManager<StreamSpec>(initialState) {

        fun checkpoint(): AirbyteStateMessage? {
            val (state: SerializableState<StreamSpec>, numRecords: Long) = swap() ?: return null
            val airbyteStreamState = AirbyteStreamState()
                .withStreamDescriptor(spec.streamDescriptor)
                .withStreamState(Jsons.jsonNode((state as SerializableStreamState).value()))
            return AirbyteStateMessage()
                .withType(AirbyteStateMessage.AirbyteStateType.STREAM)
                .withStream(airbyteStreamState)
                .withSourceStats(AirbyteStateStats().withRecordCount(numRecords.toDouble()))
        }
    }
}


