package bio.guoda.preston.cmd;

import bio.guoda.preston.store.VersionUtil;
import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

public class AnchorUtil {
    public static AtomicReference<IRI> findHead(Persisting persisting) {
        AtomicReference<IRI> head = new AtomicReference<>();

        if (persisting.isAnchored()) {
            head.set(persisting.getProvenanceAnchor());
        } else {
            resolveHead(head, persisting);
        }
        return head;
    }

    private static void resolveHead(AtomicReference<IRI> head, Persisting persisting) {
        try {
            persisting.getProvenanceTracer()
                    .trace(
                            persisting.getProvenanceAnchor(),
                            statement -> {
                                IRI iri = VersionUtil.mostRecentVersionForStatement(statement);
                                if (iri != null) {
                                    head.set(iri);
                                }
                            }
                    );
        } catch (IOException e) {
            throw new RuntimeException("Failed to get version history.", e);
        }

        if (head.get() == null) {
            throw new RuntimeException("Cannot find most recent version: no provenance logs found.");
        }
    }
}
