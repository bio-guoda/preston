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
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.RefNodeFactory.toStatement;
import static bio.guoda.preston.process.ActivityUtil.emitAsNewActivity;

public class TaxonWorksJSONExtractor extends ProcessorExtracting {
    private final Logger LOG = LoggerFactory.getLogger(TaxonWorksJSONExtractor.class);

    private final ProcessorState processorState;
    private final OutputStream outputStream;
    private final TreeMap<String, Map<Long, ObjectNode>> requestedIds = new TreeMap<>();

    public TaxonWorksJSONExtractor(ProcessorState processorState,
                                   BlobStoreReadOnly blobStoreReadOnly,
                                   OutputStream out,
                                   StatementsListener... listeners) {
        super(blobStoreReadOnly, processorState, listeners);
        this.processorState = processorState;
        this.outputStream = out;
    }


    public ContentStreamHandler getStreamHandler(BatchingEmitter batchingStatementEmitter) {
        return new TaxonWorksStreamHandler(batchingStatementEmitter);
    }


    private class TaxonWorksStreamHandler extends ContentStreamHandlerImpl implements StatementsEmitter {

        private final ContentStreamHandler handler;
        private final StatementEmitter emitter;

        public TaxonWorksStreamHandler(StatementEmitter emitter) {
            this.emitter = emitter;

            this.handler = new ContentStreamHandlerImpl(
                    new ArchiveStreamHandler(this),
                    new CompressedStreamHandler(this),
                    new TaxonWorksJSONStreamHandler(this,
                            outputStream,
                            requestedIds
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
        return "An activity that streams TaxonWorks biological association records into line-json.";
    }

}
