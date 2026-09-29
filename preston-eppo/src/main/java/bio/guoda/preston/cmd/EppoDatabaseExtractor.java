package bio.guoda.preston.cmd;

import bio.guoda.preston.process.ProcessorState;
import bio.guoda.preston.process.StatementEmitter;
import bio.guoda.preston.process.StatementsEmitter;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.stream.ContentHashDereferencer;
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

public class EppoDatabaseExtractor extends ProcessorExtracting {
    private final ProcessorState processorState;
    private final OutputStream outputStream;

    public EppoDatabaseExtractor(ProcessorState processorState,
                                 BlobStoreReadOnly blobStoreReadOnly,
                                 OutputStream out,
                                 StatementsListener... listeners) {
        super(blobStoreReadOnly, processorState, listeners);
        this.processorState = processorState;
        this.outputStream = out;
    }


    @Override
    public ContentStreamHandler getStreamHandler(BatchingEmitter batchingStatementEmitter) {
        return new EppoStreamContentHandler(batchingStatementEmitter);
    }


    private class EppoStreamContentHandler extends ContentStreamHandlerImpl implements StatementsEmitter {

        private final ContentStreamHandler handler;
        private final StatementEmitter emitter;

        public EppoStreamContentHandler(StatementEmitter emitter) {
            this.emitter = emitter;

            this.handler = new ContentStreamHandlerImpl(
                    new ArchiveStreamHandler(this),
                    new CompressedStreamHandler(this),
                    new EppoDatabaseStreamHandler(this,
                            new ContentHashDereferencer(EppoDatabaseExtractor.this),
                            outputStream
                    )
            );
        }

        @Override
        public boolean handle(IRI version, InputStream in) throws ContentStreamException {
            return handler.handle(version, in);
        }

        @Override
        public boolean handle(IRI version, InputStream in, IRI source) throws ContentStreamException {
            return handler.handle(version, in, source);
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


    @Override
    public String getActivityDescription() {
        return "An activity that streams EPPO record content into line-json.";
    }

}
