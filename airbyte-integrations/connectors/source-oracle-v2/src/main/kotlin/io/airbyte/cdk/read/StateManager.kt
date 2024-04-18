package io.airbyte.cdk.read

import io.airbyte.commons.json.Jsons
import io.airbyte.protocol.models.v0.AirbyteGlobalState
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.airbyte.protocol.models.v0.AirbyteStateStats
import io.airbyte.protocol.models.v0.AirbyteStreamState
import io.airbyte.protocol.models.v0.StreamDescriptor

class StateManager(
    initialGlobal: Pair<GlobalSpec, State<GlobalSpec>>?,
    initialStreams: Map<StreamSpec, State<StreamSpec>>,
) {

    private val globalStore: Store<GlobalSpec, SerializableGlobalState>? =
        initialGlobal?.let { Store(it.first, it.second) }

    private val streamStores: Map<StreamSpec, Store<StreamSpec, SerializableStreamState>> =
        initialStreams.mapValues { Store(it.key, it.value) }

    fun getGlobal(): Map<GlobalSpec, State<GlobalSpec>> =
        globalStore?.let { mapOf(it.spec to it.checkpointed) } ?: mapOf()

    fun getStreams(): Map<StreamSpec, State<StreamSpec>> =
        streamStores.mapValues { it.value.checkpointed }

    fun <S : Spec> update(spec: S, readState: SerializableState<S>, numRecords: Long = 0L) {
        when (readState) {
            is SerializableGlobalState -> {
                globalStore?.update(readState, numRecords)
            }
            is SerializableStreamState -> {
                val streamStore: Store<StreamSpec, SerializableStreamState>? =
                    streamStores[spec as? StreamSpec]
                if (globalStore != null) {
                    globalStore.update(numRecords)
                    streamStore?.update(readState, 0L)
                } else {
                    streamStore?.update(readState, numRecords)
                }
            }
        }
    }

    fun checkpoint(): List<AirbyteStateMessage> {
        val globalStreams: Set<StreamSpec> = globalStore?.spec?.streamSpecs?.toSet() ?: setOf()
        val (globalState, globalNumRecords) = globalStore?.checkpoint() ?: (null to 0L)
        val checkpointedStreams: Map<StreamSpec, Pair<SerializableStreamState?, Long>> =
            streamStores.mapValues { it.value.checkpoint() }
        fun streamMessage(streamSpec: StreamSpec): AirbyteStateMessage? =
            checkpointedStreams[streamSpec]?.let { (state, numRecords) ->
                state?.let {
                    val streamDescriptor = StreamDescriptor()
                        .withName(streamSpec.name)
                        .withNamespace(streamSpec.namespace)
                    val streamState = AirbyteStreamState()
                        .withStreamDescriptor(streamDescriptor)
                        .withStreamState(Jsons.jsonNode(it.value()))
                    AirbyteStateMessage()
                        .withType(AirbyteStateMessage.AirbyteStateType.STREAM)
                        .withStream(streamState)
                        .withSourceStats(AirbyteStateStats().withRecordCount(numRecords.toDouble()))
                }
            }

        val globalMessage: AirbyteStateMessage? = globalState?.let {
            val streamStates: List<AirbyteStreamState> = globalStreams.toList()
                .mapNotNull(::streamMessage)
                .map { it.stream }
            val airbyteGlobalState = AirbyteGlobalState()
                .withSharedState(Jsons.jsonNode(globalState.value()))
                .withStreamStates(streamStates)
            AirbyteStateMessage()
                .withType(AirbyteStateMessage.AirbyteStateType.GLOBAL)
                .withGlobal(airbyteGlobalState)
                .withSourceStats(AirbyteStateStats().withRecordCount(globalNumRecords.toDouble()))
        }
        val remainingStreams = checkpointedStreams.keys.filterNot { globalStreams.contains(it) }
        return listOfNotNull(globalMessage) + remainingStreams.mapNotNull(::streamMessage)
    }

    private class Store<S : Spec, T : SerializableState<S>>(
        val spec: S,
        initialState: State<S>
    ) {

        var checkpointed: State<S> = initialState
            get() = synchronized(this) { this.checkpointed }
            private set

        private var current: T? = null
        private var numRecordsSinceCheckpoint: Long = 0L

        fun update(numRecords: Long) {
            synchronized(this) {
                numRecordsSinceCheckpoint += numRecords
            }
        }

        fun update(state: T, numRecords: Long) {
            synchronized(this) {
                current = state
                numRecordsSinceCheckpoint += numRecords
            }
        }

        fun checkpoint(): Pair<T?, Long> =
            synchronized(this) {
                (current to numRecordsSinceCheckpoint).also {
                    checkpointed = current ?: checkpointed
                    current = null
                    numRecordsSinceCheckpoint = 0L
                }
            }
    }

}

