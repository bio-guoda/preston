package bio.guoda.preston.store;

import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.process.EmittingStreamRDF;
import bio.guoda.preston.process.ProcessorState;
import bio.guoda.preston.process.StatementListener;
import bio.guoda.preston.process.StatementsEmitterAdapter;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.jena.riot.RiotException;
import org.apache.jena.riot.system.ErrorHandler;

import java.io.IOException;
import java.io.InputStream;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static bio.guoda.preston.RefNodeFactory.toBlank;
import static bio.guoda.preston.RefNodeFactory.toStatement;

public class ProvenanceTrackerImpl implements ProvenanceTracker {

    private final HexaStoreReadOnly hexastore;

    private final KeyValueStoreReadOnly blobStore;

    public ProvenanceTrackerImpl(HexaStoreReadOnly hexastore) {
        this.hexastore = hexastore;
        this.blobStore = null;
    }

    public ProvenanceTrackerImpl(HexaStoreReadOnly hexastore, KeyValueStoreReadOnly blobStore) {
        this.hexastore = hexastore;
        this.blobStore = blobStore;
    }

    @Override
    public void findDescendants(IRI provenanceAnchor, StatementListener listener) throws IOException {
        VersionUtil.findMostRecentVersion(provenanceAnchor, hexastore, listener);
    }

    @Override
    public void traceOrigins(IRI provenanceAnchor, StatementListener listener) throws IOException {
        if (blobStore == null) {
            throw new UnsupportedOperationException("finding origins of provenance logs is not possible without configuring a content store");
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


        while (state.shouldKeepProcessing() && !statementQueue.isEmpty()) {
            IRI origin = statementQueue.poll();
            InputStream inputStream = blobStore.get(origin);
            if (inputStream != null) {
                StatementsEmitterAdapter emitter = new StatementsEmitterAdapter() {

                    @Override
                    public void emit(Quad statement) {
                        if (RefNodeConstants.USED_BY.equals(statement.getPredicate())) {
                            if (statement.getSubject() instanceof IRI) {
                                IRI subject = (IRI) statement.getSubject();
                                if (HashKeyUtil.isValidHashKey(subject)) {
                                    Quad originStatement =
                                            toStatement(origin,
                                                    RefNodeConstants.WAS_DERIVED_FROM,
                                                    subject);
                                    listener.on(originStatement);
                                    statementQueue.add(subject);
                                }
                            }
                        }
                    }
                };
                try {
                    new EmittingStreamRDF(emitter, state, new ErrorHandler() {
                        @Override
                        public void warning(String message, long line, long col) {
                            // ignore
                        }

                        @Override
                        public void error(String message, long line, long col) {
                            // ignore
                        }

                        @Override
                        public void fatal(String message, long line, long col) {
                            // ignore
                        }
                    })
                            .parseAndEmit(inputStream);
                    emitOriginRootIfFound(origin, listener, state, statementQueue);
                } catch (RiotException ex) {
                    // ignore opportunistic failure to parse possible provenance logs
                }
            }

        }


    }

    private void emitOriginRootIfFound(IRI provenanceAnchor, StatementListener listener, ProcessorState state, Queue<IRI> statementQueue) {
        if (state.shouldKeepProcessing() && statementQueue.isEmpty()) {
            // reached likely origin (or dead-end)
            listener.on(
                    toStatement(
                            RefNodeConstants.BIODIVERSITY_DATASET_GRAPH,
                            RefNodeConstants.HAS_VERSION,
                            provenanceAnchor)
            );
        }
    }
}
