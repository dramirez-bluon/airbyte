package io.airbyte.integrations.source.oracle

import io.airbyte.cdk.jdbc.JdbcConnectionFactory
import io.airbyte.cdk.read.CdcInitialSyncNotStarted
import io.airbyte.cdk.read.CdcInitialSyncOngoing
import io.airbyte.cdk.read.CdcInitialSyncStarting
import io.airbyte.cdk.read.CdcNotStarted
import io.airbyte.cdk.read.CdcOngoing
import io.airbyte.cdk.read.CdcStarting
import io.airbyte.cdk.read.CursorBasedIncrementalOngoing
import io.airbyte.cdk.read.CursorBasedIncrementalStarting
import io.airbyte.cdk.read.CursorBasedInitialSyncOngoing
import io.airbyte.cdk.read.CursorBasedInitialSyncStarting
import io.airbyte.cdk.read.CursorBasedNotStarted
import io.airbyte.cdk.read.FullRefreshNotStarted
import io.airbyte.cdk.read.FullRefreshResumableOngoing
import io.airbyte.cdk.read.FullRefreshResumableStarting
import io.airbyte.cdk.read.GlobalWorker
import io.airbyte.cdk.read.SerializableStreamState
import io.airbyte.cdk.read.StreamSpec
import io.airbyte.cdk.read.StreamWorker
import io.airbyte.cdk.read.WorkResult
import io.airbyte.cdk.read.Worker
import io.airbyte.cdk.read.WorkerFactory
import jakarta.inject.Singleton
import java.sql.Connection

@Singleton
class OracleWorkerFactory(
    val jdbcConnectionFactory: JdbcConnectionFactory
) : WorkerFactory {

    override fun create(input: CdcNotStarted): GlobalWorker<CdcNotStarted> {
        TODO("Not yet implemented")
    }

    override fun create(input: CdcStarting): GlobalWorker<CdcStarting> {
        TODO("Not yet implemented")
    }

    override fun create(input: CdcOngoing): GlobalWorker<CdcOngoing> {
        TODO("Not yet implemented")
    }

    override fun create(input: CdcInitialSyncNotStarted): StreamWorker<CdcInitialSyncNotStarted> {
        TODO("Not yet implemented")
    }

    override fun create(input: CdcInitialSyncStarting): StreamWorker<CdcInitialSyncStarting> {
        TODO("Not yet implemented")
    }

    override fun create(input: CdcInitialSyncOngoing): StreamWorker<CdcInitialSyncOngoing> {
        TODO("Not yet implemented")
    }

    override fun create(input: FullRefreshNotStarted): StreamWorker<FullRefreshNotStarted> {
        if (input.spec.pickedPrimaryKey != null) {
            return WorkerFactory.shortcut(input, FullRefreshResumableStarting(input.spec, input.spec.pickedPrimaryKey))
        }
    }

    override fun create(input: FullRefreshResumableStarting): StreamWorker<FullRefreshResumableStarting> {
        TODO("Not yet implemented")
    }

    override fun create(input: FullRefreshResumableOngoing): StreamWorker<FullRefreshResumableOngoing> {
        TODO("Not yet implemented")
    }

    override fun create(input: CursorBasedNotStarted): StreamWorker<CursorBasedNotStarted> {
        TODO("Not yet implemented")
    }

    override fun create(input: CursorBasedInitialSyncStarting): StreamWorker<CursorBasedInitialSyncStarting> {
        TODO("Not yet implemented")
    }

    override fun create(input: CursorBasedInitialSyncOngoing): StreamWorker<CursorBasedInitialSyncOngoing> {
        TODO("Not yet implemented")
    }

    override fun create(input: CursorBasedIncrementalStarting): StreamWorker<CursorBasedIncrementalStarting> {
        TODO("Not yet implemented")
    }

    override fun create(input: CursorBasedIncrementalOngoing): StreamWorker<CursorBasedIncrementalOngoing> {
        TODO("Not yet implemented")
    }
}
