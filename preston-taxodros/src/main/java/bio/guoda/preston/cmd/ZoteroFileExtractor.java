package bio.guoda.preston.cmd;

import bio.guoda.preston.process.StatementEmitter;
import bio.guoda.preston.process.StatementsEmitter;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.Dereferencer;
import bio.guoda.preston.stream.ArchiveStreamHandler;
import bio.guoda.preston.stream.CompressedStreamHandler;
import bio.guoda.preston.stream.ContentStreamException;
import bio.guoda.preston.stream.ContentStreamHandler;
import bio.guoda.preston.stream.ContentStreamHandlerImpl;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

public class ZoteroFileExtractor extends ProcessorExtracting {
    private final Logger LOG = LoggerFactory.getLogger(ZoteroFileExtractor.class);

    private final Persisting processorState;
    private final OutputStream outputStream;
    private final List<String> communities;
    private final IRI provenanceAnchor;

    public ZoteroFileExtractor(Persisting processorState,
                               BlobStoreReadOnly blobStoreReadOnly,
                               OutputStream out,
                               List<String> communities,
                               IRI provenanceAnchor, StatementsListener... listeners) {
        super(blobStoreReadOnly, processorState, listeners);
        this.processorState = processorState;
        this.outputStream = out;
        this.provenanceAnchor = provenanceAnchor;
        this.communities = communities;
    }


    public ContentStreamHandler getStreamHandler(BatchingEmitter batchingStatementEmitter) {
        return new ZoteroStreamHandlerImpl(batchingStatementEmitter, this);
    }


    private class ZoteroStreamHandlerImpl extends ContentStreamHandlerImpl implements StatementsEmitter {

        private final ContentStreamHandler handler;
        private final StatementEmitter emitter;

        public ZoteroStreamHandlerImpl(StatementEmitter emitter, Dereferencer<InputStream> deref) {
            this.emitter = emitter;

            this.handler = new ContentStreamHandlerImpl(
                    new ArchiveStreamHandler(this),
                    new CompressedStreamHandler(this),
                    new ZoteroFileStreamHandler(
                            this,
                            outputStream,
                            processorState,
                            deref,
                            communities,
                            provenanceAnchor
                    )
            );
        }

        @Override
        public boolean handle(IRI version, InputStream in) throws ContentStreamException {
            return handler.handle(version, in);
        }

        @Override
        public boolean shouldKeepProcessing() {
            return processorState.shouldKeepProcessing();
        }

        @Override
        public void emit(List<Quad> statements) {
            for (Quad statement : statements) {
                this.emitter.emit(statement);
            }
        }

        @Override
        public void emit(Quad statement) {
            // don't do this; statements need to be grouped to allow matches to be counted
        }
    }

    public String getActivityDescription() {
        return "An activity that streams TaxoDros Files into line-json.";
    }

}
