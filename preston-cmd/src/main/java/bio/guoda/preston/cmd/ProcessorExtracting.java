package bio.guoda.preston.cmd;

import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.process.ProcessorReadOnly;
import bio.guoda.preston.process.ProcessorState;
import bio.guoda.preston.process.StatementsEmitterAdapter;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.stream.ContentStreamException;
import bio.guoda.preston.stream.ContentStreamHandler;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static bio.guoda.preston.RefNodeConstants.DESCRIPTION;
import static bio.guoda.preston.RefNodeConstants.USED;
import static bio.guoda.preston.RefNodeFactory.getVersion;
import static bio.guoda.preston.RefNodeFactory.hasVersionAvailable;
import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.RefNodeFactory.toStatement;
import static bio.guoda.preston.process.ActivityUtil.emitAsNewActivity;

public abstract class ProcessorExtracting extends ProcessorReadOnly {
    private final Logger LOG = LoggerFactory.getLogger(ProcessorExtracting.class);

    private int emittingBatchSize = 256;

    public ProcessorExtracting(BlobStoreReadOnly blobStoreReadOnly,
                               ProcessorState processorState,
                               StatementsListener... listeners) {
        super(blobStoreReadOnly, processorState, listeners);
    }

    @Override
    public void on(Quad statement) {
        if (hasVersionAvailable(statement)) {
            IRI version = (IRI) getVersion(statement);
            final List<Quad> nodes = new ArrayList<>();

            BatchingEmitter batchingStatementEmitter = new BatchingEmitter(nodes, version, statement);
            ContentStreamHandler streamHandler = getStreamHandler(batchingStatementEmitter);
            try (InputStream in = get(version)) {
                if (in != null) {
                    try {
                        streamHandler.handle(version, in);
                    } catch (ContentStreamException ex) {
                        LOG.warn("suspicious resource [" + version.getIRIString() + "] caused errors in processing", ex);
                    }
                }
            } catch (IOException ex) {
                throw new RuntimeException("failed to get [" + version.getIRIString() + "]", ex);
            }

            // emit remaining
            batchingStatementEmitter.emitBatch();
        }
    }

    public abstract ContentStreamHandler getStreamHandler(BatchingEmitter batchingStatementEmitter);

    public class BatchingEmitter extends StatementsEmitterAdapter {

        private final List<Quad> nodes;
        private final IRI version;
        private final Quad statement;

        public BatchingEmitter(List<Quad> nodes, IRI version, Quad statement) {
            this.nodes = nodes;
            this.version = version;
            this.statement = statement;
        }

        private void emitBatch() {
            BlankNodeOrIRI newActivity = toIRI(UUID.randomUUID());
            emitAsNewActivity(
                    Stream.concat(
                            Stream.of(
                                    toStatement(newActivity, USED, version),
                                    toStatement(newActivity, DESCRIPTION, RefNodeFactory.toEnglishLiteral(ProcessorExtracting.this.getActivityDescription()))
                            ),
                            nodes.stream()
                    ),
                    ProcessorExtracting.this,
                    statement.getGraphName(),
                    newActivity);
            nodes.clear();
        }

        @Override
        public void emit(Quad statement) {
            nodes.add(statement);
            if (nodes.size() > getBatchSize()) {
                emitBatch();
            }
        }

        private int getBatchSize() {
            return emittingBatchSize;
        }
    }


    abstract public String getActivityDescription();

    public void setEmittingBatchSize(int emittingBatchSize) {
        this.emittingBatchSize = emittingBatchSize;
    }


}
