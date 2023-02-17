package bio.guoda.preston.cmd;

import bio.guoda.preston.RDFUtil;
import bio.guoda.preston.StatementLogFactory;
import bio.guoda.preston.process.EmittingStreamRDF;
import bio.guoda.preston.process.LogErrorHandler;
import bio.guoda.preston.process.StatementListener;
import bio.guoda.preston.process.StatementsEmitterAdapter;
import bio.guoda.preston.process.StatementsListener;
import org.apache.commons.rdf.api.Quad;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.IOException;
import java.io.InputStream;
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
            StatementListener handlingListener = statement -> {
                foundHistory.set(true);
                try {
                    InputStream provenance = ContentQueryUtil.getMostRecentContent(
                            resolvingBlobStore(ReplayUtil.getBlobStore(this)),
                            statement,
                            this);
                    new EmittingStreamRDF(new StatementsEmitterAdapter() {
                        @Override
                        public void emit(Quad statement) {
                            listener.on(statement);
                        }
                    }, this).parseAndEmit(provenance);
                } catch (IOException e) {
                    LOG.warn("failed to resolve content related to [" + statement.toString() + "]");
                    handler.handleError();
                }
            };
            getProvenanceTracer()
                    .trace(
                            getProvenanceAnchor(),
                            handlingListener
                    );
        } catch (IOException e) {
            throw new RuntimeException("Failed to get version history.", e);
        }

        if (!foundHistory.get()) {
            LOG.warn("No provenance found related to [" + getProvenanceAnchor().getIRIString() + "]");
        }
    }

}
