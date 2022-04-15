package bio.guoda.preston.cmd;

import bio.guoda.preston.StatementLogFactory;
import bio.guoda.preston.process.StatementsListener;
import com.beust.jcommander.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

@Parameters(separators = "= ", commandDescription = CmdHistory.SHOW_HISTORY_OF_BIODIVERSITY_DATASET_GRAPH)

@CommandLine.Command(
        name = "history",
        description = CmdHistory.SHOW_HISTORY_OF_BIODIVERSITY_DATASET_GRAPH
)
public class CmdHistory extends LoggingPersisting implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(CmdHistory.class);
    public static final String SHOW_HISTORY_OF_BIODIVERSITY_DATASET_GRAPH = "Show history of biodiversity dataset graph";

    @Override
    public void run() {
        // do not attempt to dig tiny provenance log history files out of tar.gz balls
        setSupportTarGzDiscovery(false);

        StatementsListener logger = StatementLogFactory.createPrintingLogger(
                getLogMode(),
                getOutputStream(),
                this
        );


        AtomicBoolean foundHistory = new AtomicBoolean(false);
        try {
            getTracerOfDescendants()
                    .trace(
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
