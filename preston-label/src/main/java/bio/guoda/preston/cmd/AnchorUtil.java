package bio.guoda.preston.cmd;

import bio.guoda.preston.store.VersionUtil;
import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

public class AnchorUtil {
    public static AtomicReference<IRI> findHeadOrAnchor(Persisting persisting) {
        AtomicReference<IRI> mostRecentLog = new AtomicReference<>();

        if (persisting.isAnchored()) {
            mostRecentLog.set(persisting.getProvenanceAnchor());
        } else {
            resolveHead(mostRecentLog, persisting);
        }
        return mostRecentLog;
    }

    private static void resolveHead(AtomicReference<IRI> mostRecentLog, Persisting persisting) {
        try {
            persisting.getProvenanceTracer()
                    .trace(
                            persisting.getProvenanceAnchor(),
                            statement -> {
                                IRI iri = VersionUtil.mostRecentVersionForStatement(statement);
                                if (iri != null) {
                                    mostRecentLog.set(iri);
                                }
                            }
                    );
        } catch (IOException e) {
            throw new RuntimeException("Failed to get version history.", e);
        }

        if (mostRecentLog.get() == null) {
            throw new RuntimeException("Cannot find most recent version: no provenance logs found.");
        }
    }
}
