package bio.guoda.preston.cmd;

import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.process.ProcessorReadOnly;
import bio.guoda.preston.process.ProcessorState;
import bio.guoda.preston.process.StatementEmitter;
import bio.guoda.preston.process.StatementsEmitter;
import bio.guoda.preston.process.StatementsEmitterAdapter;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.ContentHashDereferencer;
import bio.guoda.preston.stream.ArchiveStreamHandler;
import bio.guoda.preston.stream.CompressedStreamHandler;
import bio.guoda.preston.stream.ContentStreamException;
import bio.guoda.preston.stream.ContentStreamHandler;
import bio.guoda.preston.stream.ContentStreamHandlerImpl;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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

public class PlaziTreatmentExtractor extends ProcessorReadOnly {
    private final Logger LOG = LoggerFactory.getLogger(PlaziTreatmentExtractor.class);

    private final ProcessorState processorState;
    private final OutputStream outputStream;
    private int batchSize = 256;

    public PlaziTreatmentExtractor(ProcessorState processorState,
                              BlobStoreReadOnly blobStoreReadOnly,
                              OutputStream out,
                              StatementsListener... listeners) {
        super(blobStoreReadOnly, processorState, listeners);
        this.processorState = processorState;
        this.outputStream = out;
    }

    @Override
    public void on(Quad statement) {
        if (hasVersionAvailable(statement)) {
            IRI version = (IRI) getVersion(statement);
            final List<Quad> nodes = new ArrayList<>();

            BatchingEmitter batchingStatementEmitter = new BatchingEmitter(nodes, version, statement);
            MyContentStreamHandlerImpl nameReader = new MyContentStreamHandlerImpl(batchingStatementEmitter);
            try (InputStream in = get(version)) {
                if (in != null) {
                    try {
                        nameReader.handle(version, in);
                    } catch (ContentStreamException ex) {
                        LOG.warn("suspicious plazi treatment [" + version.getIRIString() + "] caused errors in processing", ex);
                    }
                }
            } catch (IOException ex) {
                throw new RuntimeException("failed to get [" + version.getIRIString() + "]", ex);
            }

            // emit remaining
            batchingStatementEmitter.emitBatch();
        }
    }


    private class MyContentStreamHandlerImpl extends ContentStreamHandlerImpl implements StatementsEmitter {

        private final ContentStreamHandler handler;
        private final StatementEmitter emitter;
        private int numMatches = 0;

        public MyContentStreamHandlerImpl(StatementEmitter emitter) {
            this.emitter = emitter;

            this.handler = new ContentStreamHandlerImpl(
                    new ArchiveStreamHandler(this),
                    new CompressedStreamHandler(this),
                    new PlaziTreatmentStreamHandler(this,
                            new ContentHashDereferencer(PlaziTreatmentExtractor.this),
                            outputStream
                    )
            );
        }

        @Override
        public boolean handle(IRI version, InputStream in) throws ContentStreamException {
            return handler.handle(version, in);
        }

        @Override
        public boolean shouldKeepReading() {
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

    private class BatchingEmitter extends StatementsEmitterAdapter {

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
                                    toStatement(newActivity, DESCRIPTION, RefNodeFactory.toEnglishLiteral(PlaziTreatmentExtractor.this.getActivityDescription()))
                            ),
                            nodes.stream()
                    ),
                    PlaziTreatmentExtractor.this,
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
            return batchSize;
        }
    }


    private String getActivityDescription() {
        return "An activity that streams Plazi content into line-json.";
    }

}