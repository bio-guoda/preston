package bio.guoda.preston.store;

import bio.guoda.preston.process.StatementListener;
import bio.guoda.preston.util.MostRecentVersionListener;
import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

public class ProvenanceTracerByIndex implements ProvenanceTracer {

    private final HexaStoreReadOnly index;

    private final ProvenanceTracer tracer;


    public ProvenanceTracerByIndex(HexaStoreReadOnly index, ProvenanceTracer tracer) {
        this.index = index;
        this.tracer = tracer;

    }

    @Override
    public void trace(IRI provenanceAnchor, StatementListener listener) throws IOException {
        AtomicReference<IRI> head = new AtomicReference<>(provenanceAnchor);
        MostRecentVersionListener versionListener = new MostRecentVersionListener(head);
        VersionUtil.findMostRecentVersion(provenanceAnchor, getIndex(), versionListener);
        tracer.trace(head.get(), listener);
    }


    public HexaStoreReadOnly getIndex() {
        return index;
    }

    @Override
    public void stopProcessing() {
        tracer.stopProcessing();
    }

    @Override
    public boolean shouldKeepProcessing() {
        return tracer.shouldKeepProcessing();
    }
}
