package bio.guoda.preston.cmd;

import bio.guoda.preston.RDFUtil;
import bio.guoda.preston.StatementLogFactory;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.store.VersionUtil;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static bio.guoda.preston.StatementLogFactory.*;

@Parameters(separators = "= ", commandDescription = "traces to origin of biodiversity dataset graphs")
public class CmdOrigins extends LoggingPersisting implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(CmdOrigins.class);

    @Parameter(description = "content id of provenance anchors (e.g., [hash://sha256/8ed311...])",
            validateWith = IRIValidator.class, converter = IRIConverter.class)
    private List<IRI> provenanceAnchors = new ArrayList<>();

    @Override
    public void run() {
        StatementsListener logger = createPrintingLogger(
                getLogMode(),
                getPrintStream(),
                this
        );


        AtomicBoolean foundHistory = new AtomicBoolean(false);
        try {

            if (provenanceAnchors.isEmpty()) {
                handleStdIn(logger, foundHistory);
            } else {
                for (IRI provenanceAnchor : provenanceAnchors) {
                    getProvenanceTracker()
                            .traceOrigins(
                                    provenanceAnchor,
                                    statement -> {
                                        foundHistory.set(true);
                                        logger.on(statement);
                                    }
                            );
                }

            }

        } catch (IOException e) {
            throw new RuntimeException("Failed to get origins.", e);
        }

        if (!foundHistory.get()) {
            LOG.warn("No origins found.");
        }
    }

    private void handleStdIn(StatementsListener logger, AtomicBoolean foundHistory) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(getInputStream()));
        String line;
        while ((line = reader.readLine()) != null) {
            Quad quad;
            try {
                quad = RDFUtil.asQuad(line);
            } catch (org.apache.jena.riot.RiotException ex) {
                // opportunistic parsing
                // skip if not able to parse first line as RDF
                break;
            }

            if (quad != null) {
                IRI provenanceAnchor = VersionUtil.mostRecentVersionForStatement(quad);
                if (provenanceAnchor != null) {
                    getProvenanceTracker()
                            .traceOrigins(
                                    provenanceAnchor,
                                    statement -> {
                                        foundHistory.set(true);
                                        logger.on(statement);
                                    }
                            );
                }
            }
        }
    }
}
