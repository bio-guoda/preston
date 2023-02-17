package bio.guoda.preston.cmd;

import bio.guoda.preston.StatementLogFactory;
import bio.guoda.preston.process.StatementsListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;


@CommandLine.Command(
        name = "history",
        aliases = {"origin", "origins", "prov", "provenance"},
        description = "Show history/origins/provenance of biodiversity dataset graph"
)
public class CmdHistory extends LoggingPersisting implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(CmdHistory.class);

    @Override
    public void run() {
        final StatementsListener logger = StatementLogFactory.createPrintingLogger(
                getLogMode(),
                getOutputStream(),
                this
        );

        AtomicBoolean foundHistory = new AtomicBoolean(false);
        try {
            getProvenanceTracer()
                    .trace(
                            getProvenanceAnchor(),
                            statement -> {
                                foundHistory.set(true);
                                logger.on(statement);
                            }
                    );
        } catch (IOException e) {
            throw new RuntimeException("Failed to get version history.", e);
        }

        if (!foundHistory.get()) {
            LOG.warn("No provenance found related to [" + getProvenanceAnchor().getIRIString() + "]");
        }
    }
}
