package bio.guoda.preston.cmd;

import bio.guoda.preston.process.ProcessorState;
import bio.guoda.preston.process.StopProcessingException;
import bio.guoda.preston.store.ProvenanceTracer;
import bio.guoda.preston.store.VersionUtil;
import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

public class AnchorUtil {

    public static AtomicReference<IRI> findHead(Persisting persisting, boolean stopProcessingOnFindingHead) {
        AtomicReference<IRI> head = new AtomicReference<>();

        if (persisting.isAnchored()) {
            head.set(persisting.getProvenanceAnchor());
        } else {
            findHead(persisting, head, stopProcessingOnFindingHead);
        }
        return head;
    }

    private static void findHead(Persisting persisting, AtomicReference<IRI> head, boolean stopProcessingOnFindingHead) {
        ProvenanceTracer provenanceTracer = persisting.getProvenanceTracer();
        IRI provenanceAnchor = persisting.getProvenanceAnchor();
        try {
            provenanceTracer
                    .trace(
                            provenanceAnchor,
                            statement -> {
                                IRI iri = VersionUtil.mostRecentVersion(statement);
                                if (iri != null) {
                                    head.set(iri);
                                    if (stopProcessingOnFindingHead) {
                                        persisting.stopProcessing();
                                    }
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
        AtomicReference<IRI> head = findHead(persisting, true);

        if (head.get() == null) {
            throw new RuntimeException("Cannot find most recent version: no provenance logs found.");
        }

        return head;
    }

}
