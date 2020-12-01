package bio.guoda.preston.process;

import bio.guoda.preston.model.RefNodeFactory;
import bio.guoda.preston.stream.ArchiveStreamHandler;
import bio.guoda.preston.stream.CompressedStreamHandler;
import bio.guoda.preston.stream.ContentStreamException;
import bio.guoda.preston.stream.ContentStreamHandler;
import bio.guoda.preston.stream.ContentStreamHandlerImpl;
import bio.guoda.preston.stream.MatchingTextStreamHandler;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static bio.guoda.preston.RefNodeConstants.DESCRIPTION;
import static bio.guoda.preston.RefNodeConstants.USED;
import static bio.guoda.preston.model.RefNodeFactory.getVersion;
import static bio.guoda.preston.model.RefNodeFactory.hasVersionAvailable;
import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static bio.guoda.preston.model.RefNodeFactory.toLiteral;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;
import static bio.guoda.preston.process.ActivityUtil.emitAsNewActivity;

public class TextMatcher extends ProcessorReadOnly {


    // From https://urlregex.com/
    public static final Pattern URL_PATTERN = Pattern.compile("(?:https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]");

    private final Pattern pattern;
    private int batchSize = 256;

    public TextMatcher(String regex, BlobStoreReadOnly blobStoreReadOnly, StatementsListener... listeners) {
        this(Pattern.compile(regex), blobStoreReadOnly, listeners);
    }

    TextMatcher(Pattern pattern, BlobStoreReadOnly blobStoreReadOnly, StatementsListener... listeners) {
        super(blobStoreReadOnly, listeners);
        this.pattern = pattern;
    }



    @Override
    public void on(Quad statement) {
        if (hasVersionAvailable(statement)) {
            IRI version = (IRI) getVersion(statement);
            final List<Quad> nodes = new ArrayList<>();

            BatchingEmitter batchingStatementEmitter = new BatchingEmitter(nodes, version, statement);
            MyContentStreamHandlerImpl textReader = new MyContentStreamHandlerImpl(batchingStatementEmitter);
            try (InputStream in = get(version)) {
                if (in != null) {
                    textReader.handle(version, in);
                }
            } catch (ContentStreamException | IOException e) {
                // ignore; this is opportunistic
            }

            // emit remaining
            batchingStatementEmitter.emitBatch();
        }
    }


    private class MyContentStreamHandlerImpl extends ContentStreamHandlerImpl {

        private final ContentStreamHandler handler;

        public MyContentStreamHandlerImpl(StatementEmitter emitter) {
            this.handler = new ContentStreamHandlerImpl(
                    new ArchiveStreamHandler(this),
                    new CompressedStreamHandler(this),
                    new MatchingTextStreamHandler(emitter, pattern));
        }

        @Override
        public boolean handle(IRI version, InputStream in) throws ContentStreamException {

            return handler.handle(version, in);
        }


    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getBatchSize() {
        return batchSize;
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
                                    toStatement(newActivity, DESCRIPTION, RefNodeFactory.toEnglishLiteral(TextMatcher.this.getActivityDescription()))
                            ),
                            nodes.stream()
                    ),
                    TextMatcher.this,
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
    }


    private String getActivityDescription() {
        return "An activity that finds the locations of text matching the regular expression '" + pattern.pattern() + "' inside any encountered content (e.g., hash://sha256/... identifiers).";
    }

}
