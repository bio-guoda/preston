package bio.guoda.preston.cmd;

import bio.guoda.preston.process.ProcessorState;
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
            final ProvenanceTracer provenanceTracer = persisting.getTracerOfDescendants();
            findHead(persisting, head, provenanceTracer);
            if (head.get() == null) {
                head.set(persisting.getProvenanceAnchor());
            }
        } else {
            findHead(persisting, head);
        }
        return head;
    }

    private static void findHead(Persisting persisting, AtomicReference<IRI> head) {
        final ProvenanceTracer provenanceTracer = persisting.getProvenanceTracer();
        findHead(persisting, head, provenanceTracer);
    }

    private static void findHead(Persisting persisting, AtomicReference<IRI> head, ProvenanceTracer provenanceTracer) {
        IRI provenanceAnchor = persisting.getProvenanceAnchor();
        try {
            provenanceTracer
                    .trace(
                            provenanceAnchor,
                            statement -> {
                                IRI iri = VersionUtil.mostRecentVersion(statement);
                                if (iri != null) {
                                    head.set(iri);
                                    provenanceTracer.stopProcessing();
                                }
                            }
                    );
        } catch (IOException e) {
            throw new RuntimeException("Failed to get version history.", e);
        } catch (StopProcessingException e) {
            if (((ProcessorState) persisting).shouldKeepProcessing()) {
                throw new RuntimeException("got unexpected stop processing exception.", e);
            }
        }
    }

    public static AtomicReference<IRI> findHeadOrThrow(Persisting persisting) {
        AtomicReference<IRI> head = findHead(persisting);

        if (head.get() == null) {
            throw new RuntimeException("Cannot find most recent version: no provenance logs found.");
        }

        return head;
    }

}
