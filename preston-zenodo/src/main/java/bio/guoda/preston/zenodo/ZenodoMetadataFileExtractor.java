package bio.guoda.preston.zenodo;

import bio.guoda.preston.cmd.ProcessorExtracting;
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
import java.util.Collection;
import java.util.List;

public class ZenodoMetadataFileExtractor extends ProcessorExtracting {
    private final Logger LOG = LoggerFactory.getLogger(ZenodoMetadataFileExtractor.class);

    private final ProcessorState processorState;
    private final ZenodoConfig zenodoContext;
    private final Collection<Quad> candidateFileDeposits;

    public ZenodoMetadataFileExtractor(CmdZenodo processorState,
                                       BlobStoreReadOnly blobStoreReadOnly,
                                       ZenodoConfig zenodoContext,
                                       Collection<Quad> candidateFileDeposits,
                                       StatementsListener... listeners) {
        super(blobStoreReadOnly, processorState, listeners);
        this.processorState = processorState;
        this.zenodoContext = zenodoContext;
        this.candidateFileDeposits = candidateFileDeposits;
        setEmitSelector(new EmitAfterZenodoRecordUpdate());
    }


    public ContentStreamHandler getStreamHandler(BatchingEmitter batchingStatementEmitter) {
        return new ZenodoStreamHandlerImpl(batchingStatementEmitter);
    }


    private class ZenodoStreamHandlerImpl extends ContentStreamHandlerImpl implements StatementsEmitter {

        private final ContentStreamHandler handler;
        private final StatementEmitter emitter;

        public ZenodoStreamHandlerImpl(StatementEmitter emitter) {
            this.emitter = emitter;

            this.handler = new ContentStreamHandlerImpl(
                    new ArchiveStreamHandler(this),
                    new CompressedStreamHandler(this),
                    new ZenodoMetadataFileStreamHandler(this,
                            new ContentHashDereferencer(ZenodoMetadataFileExtractor.this),
                            this,
                            zenodoContext,
                            ZenodoMetadataFileExtractor.this.candidateFileDeposits
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
            this.emitter.emit(statement);
        }
    }

    public String getActivityDescription() {
        return "An activity that creates or updates Zenodo records.";
    }

}
