package bio.guoda.preston.cmd;

import bio.guoda.preston.RDFUtil;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.store.KeyValueStore;
import bio.guoda.preston.store.ProvenanceTracer;
import bio.guoda.preston.store.ValidatingKeyValueStreamContentAddressedFactory;
import bio.guoda.preston.store.VersionUtil;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static bio.guoda.preston.StatementLogFactory.createPrintingLogger;

@CommandLine.Command(
        name = "origins",
        aliases = {"origin", "prov", "provenance"},
        description = "Traces to origin of biodiversity dataset graphs"
)
public class CmdOrigins extends LoggingPersisting implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(CmdOrigins.class);

    @CommandLine.Parameters(
            description = "Content id of provenance anchors (e.g., [hash://sha256/8ed311...])"
    )
    private List<IRI> provenanceAnchors = new ArrayList<>();

    @Override
    public void run() {
        KeyValueStore keyValueStore = getKeyValueStore(
                new ValidatingKeyValueStreamContentAddressedFactory(getHashType())
        );

        StatementsListener logger = createPrintingLogger(
                getLogMode(),
                getOutputStream(),
                this
        );

        AtomicBoolean foundHistory = new AtomicBoolean(false);
        try {
            ProvenanceTracer tracer = getTracerOfOrigins(keyValueStore);

            if (provenanceAnchors.isEmpty()) {
                handleStdIn(logger, foundHistory, tracer);
            } else {
                for (IRI provenanceAnchor : provenanceAnchors) {
                    handleProvAnchor(logger, foundHistory, tracer, provenanceAnchor);
                }

            }

        } catch (IOException e) {
            throw new RuntimeException("Failed to get origins.", e);
        }

        if (!foundHistory.get()) {
            LOG.warn("No origins found.");
        }
    }

    private void handleProvAnchor(StatementsListener logger, AtomicBoolean foundHistory, ProvenanceTracer tracer, IRI provenanceAnchor) throws IOException {
        tracer
                .trace(
                        provenanceAnchor,
                        statement -> {
                            foundHistory.set(true);
                            logger.on(statement);
                        }
                );
    }

    private void handleStdIn(StatementsListener logger, AtomicBoolean foundHistory, ProvenanceTracer tracer) throws IOException {
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
                    handleProvAnchor(logger, foundHistory, tracer, provenanceAnchor);
                }
            }
        }
    }
}
