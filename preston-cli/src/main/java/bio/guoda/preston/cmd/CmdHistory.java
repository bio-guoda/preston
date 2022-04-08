package bio.guoda.preston.cmd;

import bio.guoda.preston.StatementLogFactory;
import bio.guoda.preston.process.StatementsListener;
import com.beust.jcommander.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

@Parameters(separators = "= ", commandDescription = "show history of biodiversity dataset graph")
public class CmdHistory extends LoggingPersisting implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(CmdHistory.class);

    @Override
    public void run() {
        // do not attempt to dig tiny provenance log history files out of tar.gz balls
        setSupportTarGzDiscovery(false);

        StatementsListener logger = StatementLogFactory.createPrintingLogger(getLogMode(), getOutputStream(), this);


        AtomicBoolean foundHistory = new AtomicBoolean(false);
        try {

            getProvenanceTracker()
                    .findDescendants(
                            getProvenanceRoot(),
                            statement -> {
                                foundHistory.set(true);
                                logger.on(statement);
                            }
                    );

        } catch (IOException e) {
            throw new RuntimeException("Failed to get version history.", e);
        }

        if (!foundHistory.get()) {
            LOG.warn("No history found. Suggest to update first.");
        }
    }
}
