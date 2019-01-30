package bio.guoda.preston.cmd;

import bio.guoda.preston.StatementLogFactory;
import bio.guoda.preston.store.AppendOnlyBlobStore;
import bio.guoda.preston.store.BlobStore;
import bio.guoda.preston.store.StatementStore;
import bio.guoda.preston.store.StatementStoreImpl;
import com.beust.jcommander.Parameters;

import static bio.guoda.preston.cmd.ReplayUtil.attemptReplay;

@Parameters(separators = "= ", commandDescription = "list biodiversity dataset graph")
public class CmdList extends LoggingPersisting implements Runnable {

    @Override
    public void run() {
        BlobStore blobStore = new AppendOnlyBlobStore(getKeyValueStore());
        StatementStore statementPersistence = new StatementStoreImpl(getKeyValueStore());
        attemptReplay(blobStore, statementPersistence, StatementLogFactory.createLogger(getLogMode()));
    }

}
