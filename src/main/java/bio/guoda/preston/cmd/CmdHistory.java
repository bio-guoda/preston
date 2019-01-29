package bio.guoda.preston.cmd;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import bio.guoda.preston.StatementLogFactory;
import bio.guoda.preston.model.RefNodeFactory;
import bio.guoda.preston.process.StatementListener;
import bio.guoda.preston.store.StatementStore;
import bio.guoda.preston.store.StatementStoreImpl;
import bio.guoda.preston.store.VersionUtil;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static bio.guoda.preston.RefNodeConstants.ARCHIVE;
import static bio.guoda.preston.RefNodeConstants.ARCHIVE_COLLECTION;
import static bio.guoda.preston.model.RefNodeFactory.toBlank;

@Parameters(separators = "= ", commandDescription = "show history of biodiversity resource")
public class CmdHistory extends LoggingPersisting implements Runnable {

    private static final Log LOG = LogFactory.getLog(CmdHistory.class);


    @Override
    public void run() {
        StatementListener logger = StatementLogFactory.createLogger(getLogMode());
        StatementStore statementStore = new StatementStoreImpl(getCrawlRelationsStore());
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
            throw new RuntimeException("Failed to get version history.");
        }

        if (!foundHistory.get()) {
            LOG.warn("No history found. Suggest to update first.");
        }
    }
}
