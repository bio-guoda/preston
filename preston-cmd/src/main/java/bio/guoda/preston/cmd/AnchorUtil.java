package bio.guoda.preston.cmd;

import bio.guoda.preston.process.StopProcessingException;
import bio.guoda.preston.store.ProvenanceTracer;
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

    public static AtomicReference<IRI> findHeadOrThrow(Persisting persisting) {
        AtomicReference<IRI> head = findHead(persisting);

        if (head.get() == null) {
            throw new RuntimeException("Cannot find most recent version: no provenance logs found.");
        }

        return head;
    }

    private static void resolveHead(AtomicReference<IRI> head, Persisting persisting) {
        ProvenanceTracer provenanceTracer = persisting.getProvenanceTracer();
        IRI provenanceAnchor = persisting.getProvenanceAnchor();
        findHead(head, provenanceTracer, provenanceAnchor, persisting);
    }

    private static void findHead(AtomicReference<IRI> head, ProvenanceTracer provenanceTracer, IRI provenanceAnchor, Persisting persisting) {
        try {
            provenanceTracer
                    .trace(
                            provenanceAnchor,
                            statement -> {
                                IRI iri = VersionUtil.mostRecentVersionForStatement(statement);
                                if (iri != null) {
                                    head.set(iri);
                                    persisting.stopProcessing();
                                }
                            }
                    );
        } catch (IOException e) {
            throw new RuntimeException("Failed to get version history.", e);
        } catch (StopProcessingException e) {
            if (persisting.shouldKeepProcessing()) {
                throw new RuntimeException("got unexpected stop processing exception.", e);
            }
        }
    }
}
