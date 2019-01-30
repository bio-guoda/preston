package bio.guoda.preston.cmd;

import bio.guoda.preston.store.AppendOnlyBlobStore;
import bio.guoda.preston.store.BlobStore;
import bio.guoda.preston.store.StatementStore;
import bio.guoda.preston.store.StatementStoreImpl;
import com.beust.jcommander.Parameters;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import static bio.guoda.preston.cmd.ReplayUtil.attemptReplay;

@Parameters(separators = "= ", commandDescription = "list biodiversity dataset graph")
public class CmdList extends LoggingPersisting implements Runnable {

    private static final Log LOG = LogFactory.getLog(CmdList.class);

    @Override
    public void run() {
        BlobStore blobStore = new AppendOnlyBlobStore(getBlobPersistence());

        StatementStore statementPersistence = new StatementStoreImpl(getCrawlRelationsStore());
        attemptReplay(blobStore, statementPersistence, getLogMode());
    }

}
