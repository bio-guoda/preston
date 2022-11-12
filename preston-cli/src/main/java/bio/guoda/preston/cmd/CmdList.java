package bio.guoda.preston.cmd;

import bio.guoda.preston.StatementLogFactory;
import bio.guoda.preston.process.LogErrorHandler;
import bio.guoda.preston.process.StatementsListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

@CommandLine.Command(
        name = "ls",
        aliases = {"log", "logs"},
        description = "Show biodiversity dataset provenance logs"
)
public class CmdList extends LoggingPersisting implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(CmdList.class);


    @Override
    public void run() {
        run(LogErrorHandlerExitOnError.EXIT_ON_ERROR);
    }

    public void run(LogErrorHandler handler) {


        StatementsListener listener = StatementLogFactory
                .createPrintingLogger(
                        getLogMode(),
                        getOutputStream(),
                        handler);

        AtomicBoolean foundHistory = new AtomicBoolean(false);
        try {
            getProvenanceTracer()
                    .trace(
                            getProvenanceAnchor(),
                            statement -> {
                                foundHistory.set(true);
                                try {
                                    ContentQueryUtil.handleContentQuery(
                                            resolvingBlobStore(ReplayUtil.getBlobStore(this)),
                                            statement,
                                            this);
                                } catch (IOException e) {
                                    LOG.warn("falied to resolve content related to [" + statement.toString() + "]");
                                    handler.handleError();
                                }
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
