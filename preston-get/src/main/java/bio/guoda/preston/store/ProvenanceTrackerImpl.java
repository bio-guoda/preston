package bio.guoda.preston.store;

import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.process.EmittingStreamRDF;
import bio.guoda.preston.process.ProcessorState;
import bio.guoda.preston.process.StatementListener;
import bio.guoda.preston.process.StatementsEmitterAdapter;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;

import java.io.IOException;
import java.io.InputStream;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static bio.guoda.preston.RefNodeFactory.toBlank;
import static bio.guoda.preston.RefNodeFactory.toStatement;

public class ProvenanceTrackerImpl implements ProvenanceTracker {

    private final HexaStoreReadOnly hexastore;

    private final BlobStoreReadOnly blobStore;

    public ProvenanceTrackerImpl(HexaStoreReadOnly hexastore) {
        this.hexastore = hexastore;
        this.blobStore = null;
    }

    public ProvenanceTrackerImpl(HexaStoreReadOnly hexastore, BlobStoreReadOnly blobStore) {
        this.hexastore = hexastore;
        this.blobStore = blobStore;
    }

    @Override
    public void findDescendants(IRI provenanceAnchor, StatementListener listener) throws IOException {
        VersionUtil.findMostRecentVersion(provenanceAnchor, hexastore, listener);
    }

    @Override
    public void findOrigins(IRI provenanceAnchor, StatementListener listener) throws IOException {
        if (blobStore == null) {
            throw new UnsupportedOperationException("finding origins of provenance logs is not possible (yet) with this hexastore-based provenance tracker");
        }


        ProcessorState state = new ProcessorState() {

            @Override
            public boolean shouldKeepProcessing() {
                return true;
            }

            @Override
            public void stopProcessing() {

            }
        };

        final Queue<IRI> statementQueue =
                new ConcurrentLinkedQueue<IRI>() {{
                    add(provenanceAnchor);
                }};


        StatementsEmitterAdapter emitter = new StatementsEmitterAdapter() {

            @Override
            public void emit(Quad statement) {
                if (RefNodeConstants.USED_BY.equals(statement.getPredicate())) {
                    if (statement.getSubject() instanceof IRI) {
                        IRI subject = (IRI) statement.getSubject();
                        if (HashKeyUtil.isValidHashKey(subject)) {
                            listener.on(statement);
                            statementQueue.add(subject);
                        }
                    }
                }
            }
        };

        while (state.shouldKeepProcessing() && !statementQueue.isEmpty()) {
            InputStream inputStream = blobStore.get(statementQueue.poll());
            if (inputStream != null) {
                new EmittingStreamRDF(emitter, state)
                        .parseAndEmit(inputStream);
            }

        }


    }
}
