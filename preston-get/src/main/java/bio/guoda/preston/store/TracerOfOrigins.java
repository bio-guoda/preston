package bio.guoda.preston.store;

import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.process.EmittingStreamRDF;
import bio.guoda.preston.process.ProcessorState;
import bio.guoda.preston.process.StatementListener;
import bio.guoda.preston.process.StatementsEmitterAdapter;
import org.apache.commons.rdf.api.IRI;
import org.apache.jena.riot.RiotException;
import org.apache.jena.riot.system.ErrorHandler;

import java.io.IOException;
import java.io.InputStream;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static bio.guoda.preston.RefNodeFactory.toStatement;

public class TracerOfOrigins implements ProvenanceTracer {

    private final KeyValueStoreReadOnly blobStore;

    private final ProcessorState cmd;

    public TracerOfOrigins(KeyValueStoreReadOnly blobStore, ProcessorState cmd) {
        this.blobStore = blobStore;
        this.cmd = cmd;

    }

    @Override
    public void trace(IRI provenanceAnchor, StatementListener listener) throws IOException {
        if (getBlobStore() == null) {
            throw new UnsupportedOperationException("finding origins of provenance logs is not possible without configuring a content store");
        }

        final Queue<IRI> statementQueue =
                new ConcurrentLinkedQueue<IRI>() {{
                    add(provenanceAnchor);
                }};


        while (cmd.shouldKeepProcessing() && !statementQueue.isEmpty()) {
            IRI someOrigin = statementQueue.poll();
            InputStream inputStream = getBlobStore().get(someOrigin);
            if (inputStream != null) {
                StatementsEmitterAdapter emitter = new EmitsDerivedFrom(
                        someOrigin,
                        listener,
                        statementQueue
                );
                try {
                    new EmittingStreamRDF(emitter, cmd, new ErrorHandlerNOOP())
                            .parseAndEmit(inputStream);

                    emitOriginRootIfFound(someOrigin, listener, cmd, statementQueue);
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

    public KeyValueStoreReadOnly getBlobStore() {
        return blobStore;
    }

    private static class ErrorHandlerNOOP implements ErrorHandler {
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
    }
}
