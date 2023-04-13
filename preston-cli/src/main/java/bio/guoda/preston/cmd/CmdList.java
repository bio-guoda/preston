package bio.guoda.preston.cmd;

import bio.guoda.preston.StatementLogFactory;
import bio.guoda.preston.process.LogErrorHandler;
import bio.guoda.preston.process.StatementsListener;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
        CopyShop copyShop = LogTypes.nquads.equals(getLogMode())
                ? new CopyShopImpl()
                : new CopyShopNQuadToTSV(this);


        AtomicBoolean foundHistory = new AtomicBoolean(false);
        try {
            getProvenanceTracer()
                    .trace(
                            getProvenanceAnchor(),
                            statement -> {
                                foundHistory.set(true);
                                try {
                                    ContentQueryUtil.copyMostRecentContent(
                                            resolvingBlobStore(ReplayUtil.getBlobStore(this)),
                                            statement,
                                            this,
                                            copyShop);
                                } catch (IOException e) {
                                    LOG.warn("failed to resolve content related to [" + statement.toString() + "]");
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
