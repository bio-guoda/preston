package bio.guoda.preston.cmd;

import bio.guoda.preston.process.ProcessorState;
import bio.guoda.preston.process.StatementEmitter;
import bio.guoda.preston.process.StatementsEmitter;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.ContentHashDereferencer;
import bio.guoda.preston.stream.ArchiveStreamHandler;
import bio.guoda.preston.stream.CompressedStreamHandler;
import bio.guoda.preston.stream.ContentStreamException;
import bio.guoda.preston.stream.ContentStreamHandler;
import bio.guoda.preston.stream.ContentStreamHandlerImpl;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.gbif.dwc.DwCArchiveStreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

public class DwcRecordExtractor extends ProcessorExtracting {
    private final Logger LOG = LoggerFactory.getLogger(DwcRecordExtractor.class);

    private final ProcessorState processorState;
    private final OutputStream outputStream;
    private int batchSize = 256;

    public DwcRecordExtractor(ProcessorState processorState,
                              BlobStoreReadOnly blobStoreReadOnly,
                              OutputStream out,
                              StatementsListener... listeners) {
        super(blobStoreReadOnly, processorState, listeners);
        this.processorState = processorState;
        this.outputStream = out;
    }

    @Override
    public ContentStreamHandler getStreamHandler(BatchingEmitter batchingStatementEmitter) {
        return new DwCStreamHandlerImpl(batchingStatementEmitter);
    }


    private class DwCStreamHandlerImpl extends ContentStreamHandlerImpl implements StatementsEmitter {

        private final ContentStreamHandler handler;
        private final StatementEmitter emitter;
        private int numMatches = 0;

        public DwCStreamHandlerImpl(StatementEmitter emitter) {
            this.emitter = emitter;

            this.handler = new ContentStreamHandlerImpl(
                    new ArchiveStreamHandler(this),
                    new CompressedStreamHandler(this),
                    new DwCArchiveStreamHandler(this,
                            new ContentHashDereferencer(DwcRecordExtractor.this),
                            outputStream
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
            ++numMatches;
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
        return "An activity that streams DwC-A content into line-json.";
    }

}
