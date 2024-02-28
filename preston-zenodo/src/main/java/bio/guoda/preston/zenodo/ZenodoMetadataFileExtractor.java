package bio.guoda.preston.zenodo;

import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.cmd.EmitSelector;
import bio.guoda.preston.cmd.ProcessorExtracting;
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
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

public class ZenodoMetadataFileExtractor extends ProcessorExtracting {
    private final Logger LOG = LoggerFactory.getLogger(ZenodoMetadataFileExtractor.class);

    private final ProcessorState processorState;
    private final ZenodoContext zenodoContext;

    public ZenodoMetadataFileExtractor(CmdZenodo processorState,
                                       BlobStoreReadOnly blobStoreReadOnly,
                                       ZenodoContext zenodoContext,
                                       StatementsListener... listeners) {
        super(blobStoreReadOnly, processorState, listeners);
        this.processorState = processorState;
        this.zenodoContext = zenodoContext;
        setEmitSelector(new EmitAfterZenodoRecordUpdate());
    }


    public ContentStreamHandler getStreamHandler(BatchingEmitter batchingStatementEmitter) {
        return new ZenodoStreamHandlerImpl(batchingStatementEmitter);
    }

    private static class EmitAfterZenodoRecordUpdate implements EmitSelector {
        @Override
        public boolean shouldEmit(List<Quad> nodes) {
            long count = nodes.stream()
                    .filter(q -> StringUtils.equals(RefNodeConstants.LAST_REFRESHED_ON.toString(), q.getPredicate().getIRIString()))
                    .count();
            return count > 0 || nodes.size() > 256;
        }
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
                            zenodoContext
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
