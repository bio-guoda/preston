package bio.guoda.preston.cmd;

import bio.guoda.preston.store.KeyValueStore;
import bio.guoda.preston.store.KeyValueStoreCopying;
import bio.guoda.preston.store.KeyValueStoreLocalFileSystem;
import bio.guoda.preston.store.KeyValueStoreRemoteHTTP;
import com.beust.jcommander.Parameters;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import bio.guoda.preston.StatementLogFactory;
import bio.guoda.preston.process.StatementListener;
import bio.guoda.preston.store.StatementStore;
import bio.guoda.preston.store.StatementStoreImpl;
import bio.guoda.preston.store.VersionUtil;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static bio.guoda.preston.RefNodeConstants.ARCHIVE;
import static bio.guoda.preston.model.RefNodeFactory.toBlank;

@Parameters(separators = "= ", commandDescription = "show history of biodiversity dataset graph")
public class CmdHistory extends LoggingPersisting implements Runnable {

    private static final Log LOG = LogFactory.getLog(CmdHistory.class);


    @Override
    public void run() {
        StatementListener logger = StatementLogFactory.createLogger(getLogMode());

        StatementStore statementStore = new StatementStoreImpl(getKeyValueStore());
        AtomicBoolean foundHistory = new AtomicBoolean(false);
        try {
            VersionUtil.findMostRecentVersion(
                    ARCHIVE
                    , statementStore
                    , statement -> {
                        foundHistory.set(true);
                        logger.on(statement);
                    });
        } catch (IOException e) {
            throw new RuntimeException("Failed to get version history.", e);
        }

        if (!foundHistory.get()) {
            LOG.warn("No history found. Suggest to update first.");
        }
    }
}
