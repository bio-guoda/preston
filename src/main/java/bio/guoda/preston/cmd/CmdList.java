package bio.guoda.preston.cmd;

import bio.guoda.preston.StatementLogFactory;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.BlobStore;
import bio.guoda.preston.store.StatementStore;
import bio.guoda.preston.store.StatementStoreImpl;
import com.beust.jcommander.Parameters;

import static bio.guoda.preston.cmd.ReplayUtil.attemptReplay;

@Parameters(separators = "= ", commandDescription = "show biodiversity data provenance logs")
public class CmdList extends LoggingPersisting implements Runnable {

    @Override
    public void run() {
        run(new LogErrorHandler() {
            @Override
            public void handleError() {
                System.exit(0);
            }
        });
    }

    public void run(LogErrorHandler handler) {
        BlobStore blobStore = new BlobStoreAppendOnly(getKeyValueStore());
        StatementStore statementPersistence = new StatementStoreImpl(getKeyValueStore());
        attemptReplay(blobStore, statementPersistence, StatementLogFactory.createLogger(getLogMode(), System.out, handler));
    }

}
