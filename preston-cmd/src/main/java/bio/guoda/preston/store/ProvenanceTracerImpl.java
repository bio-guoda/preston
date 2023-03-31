package bio.guoda.preston.store;

import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.process.EmittingStreamOfAnyVersions;
import bio.guoda.preston.process.EmittingStreamOfUsedByVersions;
import bio.guoda.preston.process.ProcessorState;
import bio.guoda.preston.process.StatementListener;
import bio.guoda.preston.process.StatementsEmitterAdapter;
import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static bio.guoda.preston.RefNodeFactory.toStatement;

public class ProvenanceTracerImpl implements ProvenanceTracer {

    private final KeyValueStoreReadOnly blobStore;

    private final AtomicBoolean keepProcessing = new AtomicBoolean(true);

    public ProvenanceTracerImpl(KeyValueStoreReadOnly blobStore, ProcessorState cmd) {
        this.blobStore = blobStore;
    }

    @Override
    public void trace(IRI provenanceAnchor, StatementListener listener) throws IOException {
        if (getBlobStore() == null) {
            throw new UnsupportedOperationException("finding origins is not possible without configuring a content store");
        }

        final Queue<IRI> statementQueue =
                new ConcurrentLinkedQueue<IRI>() {{
                    if (HashKeyUtil.isValidHashKey(provenanceAnchor)) {
                        add(provenanceAnchor);
                    }
                }};


        while (shouldKeepProcessing() && !statementQueue.isEmpty()) {
            IRI someOrigin = statementQueue.poll();
            InputStream inputStream = getBlobStore().get(someOrigin);
            if (inputStream != null) {
                List<IRI> discoveredStatements = new ArrayList<>();
                StatementsEmitterAdapter emitter = new EmitsDerivedFrom(
                        someOrigin,
                        listener,
                        discoveredStatements
                );
                new EmittingStreamOfUsedByVersions(
                        emitter,
                        this
                ).parseAndEmit(inputStream);

                if (discoveredStatements.size() == 0) {
                    listener.on(
                            toStatement(
                                    RefNodeConstants.BIODIVERSITY_DATASET_GRAPH,
                                    RefNodeConstants.HAS_VERSION,
                                    someOrigin)
                    );
                }
                statementQueue.addAll(discoveredStatements);

            }

        }


    }

    public KeyValueStoreReadOnly getBlobStore() {
        return blobStore;
    }

    @Override
    public void stopProcessing() {
        keepProcessing.set(false);
    }

    @Override
    public boolean shouldKeepProcessing() {
        return keepProcessing.get();
    }
}
