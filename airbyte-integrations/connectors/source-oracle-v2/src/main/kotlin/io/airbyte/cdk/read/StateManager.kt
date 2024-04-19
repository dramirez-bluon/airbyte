package io.airbyte.cdk.read

import io.airbyte.cdk.command.StreamStateValue
import io.airbyte.commons.json.Jsons
import io.airbyte.protocol.models.v0.AirbyteGlobalState
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.airbyte.protocol.models.v0.AirbyteStateStats
import io.airbyte.protocol.models.v0.AirbyteStreamState

class StateManager(
    initialGlobal: Pair<GlobalSpec, State<GlobalSpec>>?,
    initialStreams: Map<StreamSpec, State<StreamSpec>>,
) {

    private val global: GlobalStateManager?
    private val nonGlobal: Map<StreamSpec, NonGlobalStreamStateManager>

    init {
        if (initialGlobal == null) {
            global = null
            nonGlobal = initialStreams.mapValues { NonGlobalStreamStateManager(it.key, it.value) }
        } else {
            val globalStreams: Map<StreamSpec, State<StreamSpec>> =
                initialGlobal.first.streamSpecs.associateWith { initialStreams[it]!! }
            global = GlobalStateManager(
                globalSpec = initialGlobal.first,
                initialGlobalState = initialGlobal.second,
                initialStreamStates = globalStreams)
            nonGlobal = initialStreams
                .filterKeys { !globalStreams.containsKey(it) }
                .mapValues { NonGlobalStreamStateManager(it.key, it.value) }
        }
    }

    fun get(): List<WorkSpec<out Spec,*>> =
        listOfNotNull(global?.workSpec()) +
            (global?.streamStateManagers?.values ?: listOf()).mapNotNull { it.workSpec() } +
            nonGlobal.values.mapNotNull { it.workSpec() }

    fun <S : Spec, I : SelectableState<S>, O : SerializableState<S>> set(wr: WorkResult<S, I, O>) {
        when (wr.output) {
            is SerializableGlobalState -> global?.set(wr.output, wr.numRecords)
            is SerializableStreamState -> {
                val streamSpec: StreamSpec = wr.workSpec.spec as StreamSpec
                global?.streamStateManagers?.get(streamSpec)?.set(wr.output, wr.numRecords)
                nonGlobal[streamSpec]?.set(wr.output, wr.numRecords)
            }
        }
    }

    fun checkpoint(): List<AirbyteStateMessage> =
        listOfNotNull(global?.checkpoint()) + nonGlobal.mapNotNull {it.value.checkpoint() }

    private sealed class BaseStateManager<S : Spec>(
        val spec: S,
        initialState: State<S>,
        private val isCheckpointUnique: Boolean = true
    ) {
        private var current: State<S> = initialState
        private var pending: State<S> = initialState
        private var pendingNumRecords: Long = 0L

        fun workSpec(): WorkSpec<S,*>? =
            when (val state: State<S> = synchronized(this) { current }) {
                is SelectableState<S> -> WorkSpec(spec, state)
                else -> null
            }

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
        globalSpec: GlobalSpec,
        initialGlobalState: State<GlobalSpec>,
        initialStreamStates: Map<StreamSpec, State<StreamSpec>>
    ) : BaseStateManager<GlobalSpec>(globalSpec, initialGlobalState) {

        val streamStateManagers: Map<StreamSpec, GlobalStreamStateManager> =
            initialStreamStates.mapValues { (spec, state) ->
                GlobalStreamStateManager(spec, state)
            }

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
        streamSpec: StreamSpec,
        initialState: State<StreamSpec>
    ) : BaseStateManager<StreamSpec>(streamSpec, initialState, isCheckpointUnique = false) {

        fun checkpointGlobalStream(): Pair<AirbyteStreamState, Long> {
            val (state: SerializableState<StreamSpec>?, numRecords: Long) = swap() ?: (null to 0L)
            val value: StreamStateValue? = (state as? SerializableStreamState)?.value()
            return AirbyteStreamState()
                .withStreamDescriptor(spec.streamDescriptor)
                .withStreamState(value?.let { Jsons.jsonNode(it) }) to numRecords
        }
    }


    private class NonGlobalStreamStateManager(
        streamSpec: StreamSpec,
        initialState: State<StreamSpec>
    ) : BaseStateManager<StreamSpec>(streamSpec, initialState) {

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

