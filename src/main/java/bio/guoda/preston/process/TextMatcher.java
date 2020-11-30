package bio.guoda.preston.process;

import bio.guoda.preston.model.RefNodeFactory;
import bio.guoda.preston.stream.ContentStreamException;
import bio.guoda.preston.stream.ContentStreamHandlerImpl;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import sun.nio.cs.ThreadLocalCoders;

import java.io.IOException;
import java.io.InputStream;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CodingErrorAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static bio.guoda.preston.RefNodeConstants.DESCRIPTION;
import static bio.guoda.preston.RefNodeConstants.HAD_MEMBER;
import static bio.guoda.preston.RefNodeConstants.HAS_VALUE;
import static bio.guoda.preston.RefNodeConstants.USED;
import static bio.guoda.preston.model.RefNodeFactory.getVersion;
import static bio.guoda.preston.model.RefNodeFactory.hasVersionAvailable;
import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static bio.guoda.preston.model.RefNodeFactory.toLiteral;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;
import static bio.guoda.preston.process.ActivityUtil.emitAsNewActivity;

public class TextMatcher extends ProcessorReadOnly {

    private static final int BUFFER_SIZE = 4096;
    private static final int MAX_MATCH_SIZE_IN_BYTES = 512;

    // From https://urlregex.com/
    public static final Pattern URL_PATTERN = Pattern.compile("(?:https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]");

    private final Pattern pattern;
    private int batchSize = 256;

    private Map<Integer, String> patternGroupNames;

    public TextMatcher(String regex, BlobStoreReadOnly blobStoreReadOnly, StatementsListener... listeners) {
        this(Pattern.compile(regex), blobStoreReadOnly, listeners);
    }

    public TextMatcher(Pattern pattern, BlobStoreReadOnly blobStoreReadOnly, StatementsListener... listeners) {
        super(blobStoreReadOnly, listeners);
        this.pattern = pattern;
        this.patternGroupNames = extractPatternGroupNames(pattern);
    }

    protected static Map<Integer, String> extractPatternGroupNames(Pattern pattern) {

        Pattern sterilizedPattern = sterilizePatternForGroupDetection(pattern);

        final Pattern matchRegexGroupNames = Pattern.compile("\\((?:\\?<([a-zA-Z][a-zA-Z0-9]*)>|[^?)])");
        Matcher matcher = matchRegexGroupNames.matcher(sterilizedPattern.pattern());

        Map<Integer, String> patternGroupNames = new HashMap<>();
        for (int i = 1; matcher.find(); ++i) {
            String groupName = matcher.group(1);
            if (groupName != null) {
                patternGroupNames.put(i, groupName);
            }
        }

        return patternGroupNames;
    }

    protected static Pattern sterilizePatternForGroupDetection(Pattern pattern) {
        final Pattern matchRegexEscapes = Pattern.compile("\\\\.");
        final Pattern matchRegexClasses = Pattern.compile("\\[[^\\[\\]]+]");

        AtomicReference<String> sterilizedPattern = new AtomicReference<>(pattern.pattern());
        Stream.of(
                matchRegexEscapes,
                matchRegexClasses
        ).forEach(
                sterilizerPattern -> sterilizedPattern.set(
                        deepReplaceAll(sterilizedPattern.get(), sterilizerPattern, ".")
                )
        );

        return Pattern.compile(sterilizedPattern.get());
    }

    private static String deepReplaceAll(String string, Pattern pattern, String replacement) {
        String newString = pattern.matcher(string).replaceAll(replacement);
        if (newString.equals(string)) {
            return string;
        }
        else {
            return deepReplaceAll(newString, pattern, replacement);
        }
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

    private static IRI getCutIri(IRI fileIri, int startAt, int endAt) {
        return toIRI(String.format("cut:%s!/b%d-%d", fileIri.getIRIString(), startAt + 1, endAt));
    }

    private int GetBufferPosition(Buffer buffer) {
        return buffer.position();
    }

    private void SetBufferPosition(Buffer buffer, int newPosition) {
        buffer.position(newPosition);
    }

    private class MyContentStreamHandlerImpl extends ContentStreamHandlerImpl {

        private final StatementEmitter emitter;

        public MyContentStreamHandlerImpl(StatementEmitter emitter) {
            this.emitter = emitter;
        }

        protected void parseAsText(IRI version, InputStream in, Charset charset) throws IOException {
            byte[] byteBuffer = new byte[BUFFER_SIZE];

            int offset = 0;
            int numBytesToReuse = 0;
            int numBytesScannedInLastIteration = 0;
            while (true) {
                int numBytesToScan = numBytesToReuse;

                // Copy text from the end of the buffer to the beginning in case matches occur across buffer boundaries
                System.arraycopy(byteBuffer, numBytesScannedInLastIteration - numBytesToReuse, byteBuffer, 0, numBytesToReuse);

                int numBytesRead;
                while ((numBytesRead = in.read(byteBuffer, numBytesToScan, byteBuffer.length - numBytesToScan)) > 0)
                {
                    numBytesToScan += numBytesRead;
                }

                if (numBytesToScan <= 0) {
                    break;
                }
                ByteBuffer scanningByteBuffer = ByteBuffer.wrap(byteBuffer, 0, numBytesToScan);

                // Default CharBuffer::decode behavior is to replace uninterpretable bytes with an "unknown" character that
                // is not always the same length in bytes. Instead, ignore those bytes and reinsert them after encoding.
                CharBuffer charBuffer = ThreadLocalCoders.decoderFor(charset)
                        .onMalformedInput(CodingErrorAction.IGNORE)
                        .onUnmappableCharacter(CodingErrorAction.IGNORE)
                        .decode(scanningByteBuffer);

                SetBufferPosition(scanningByteBuffer, 0);

                Matcher matcher = pattern.matcher(charBuffer);
                CharBufferByteReader charBufferByteReader = new CharBufferByteReader(scanningByteBuffer, charBuffer, charset);

                while (matcher.find()) {
                    int charPosMatchStartsAt = matcher.start();
                    int bytePosMatchStartsAt = charBufferByteReader.advance(charPosMatchStartsAt);

                    if (bytePosMatchStartsAt >= BUFFER_SIZE - MAX_MATCH_SIZE_IN_BYTES) {
                        SetBufferPosition(scanningByteBuffer, bytePosMatchStartsAt);
                        break;
                    }

                    List<Integer> orderedCharPositions = new LinkedList<>();
                    for (int i = 0; i <= matcher.groupCount(); ++i) {
                        if (matcher.group(i) != null) {
                            orderedCharPositions.add(matcher.start(i));
                            orderedCharPositions.add(matcher.end(i));
                        }
                    }

                    orderedCharPositions.sort(null);

                    // Because characters can have variable width, report byte positions instead of character positions
                    Map<Integer, Integer> charToBytePositions = orderedCharPositions.stream().distinct().collect(Collectors.toMap(
                            charPosition -> charPosition,
                            charBufferByteReader::advance
                    ));

                    int charPosMatchEndsAt = matcher.end();
                    int bytePosMatchEndsAt = charToBytePositions.get(charPosMatchEndsAt);
                    IRI matchIri = getCutIri(version, offset + bytePosMatchStartsAt, offset + bytePosMatchEndsAt);

                    for (int i = 0; i <= matcher.groupCount(); ++i) {
                        if (matcher.group(i) != null) {
                            int charPosGroupStartsAt = matcher.start(i);
                            int charPosGroupEndsAt = matcher.end(i);

                            int bytePosGroupStartsAt = charToBytePositions.get(charPosGroupStartsAt);
                            int bytePosGroupEndsAt = charToBytePositions.get(charPosGroupEndsAt);

                            String groupString = matcher.group(i);
                            IRI groupIri = getCutIri(version, offset + bytePosGroupStartsAt, offset + bytePosGroupEndsAt);
                            emitter.emit(toStatement(groupIri, HAS_VALUE, toLiteral(groupString)));

                            if (i > 0) {
                                emitter.emit(toStatement(matchIri, HAD_MEMBER, groupIri));

                                if (patternGroupNames.containsKey(i)) {
                                    String groupName = patternGroupNames.get(i);
                                    if (groupString.equals(matcher.group(groupName))) {
                                        emitter.emit(toStatement(matchIri, DESCRIPTION, toLiteral(groupName)));
                                    }
                                    else {
                                        throw new RuntimeException("pattern group [" + groupName + "] was assigned the wrong index");
                                    }
                                }
                            }
                        }
                    }
                }

                // If no matches were found, we need to advance the buffer's position manually
                if (GetBufferPosition(scanningByteBuffer) == 0) {
                    SetBufferPosition(scanningByteBuffer, scanningByteBuffer.limit());
                }

                numBytesScannedInLastIteration = numBytesToScan;
                numBytesToReuse = Integer.min(MAX_MATCH_SIZE_IN_BYTES, numBytesScannedInLastIteration - GetBufferPosition(scanningByteBuffer));
                offset += numBytesScannedInLastIteration - numBytesToReuse;
            }
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

    private class CharBufferByteReader {
        private ByteBuffer byteBuffer;
        private CharBuffer charBuffer;
        private Charset charset;

        private int charPosition = 0;

        public CharBufferByteReader(ByteBuffer scanningByteBuffer, CharBuffer charBuffer, Charset charset) {
            this.byteBuffer = scanningByteBuffer;
            this.charBuffer = charBuffer;
            this.charset = charset;
        }

        int advance(int newCharPosition) {
            ByteBuffer filteredByteBuffer = charset.encode(charBuffer.subSequence(this.charPosition, newCharPosition));

            int i = GetBufferPosition(byteBuffer);
            for (int j = 0; i < byteBuffer.limit() && j < filteredByteBuffer.limit(); ++i) {
                if (byteBuffer.get(i) == filteredByteBuffer.get(j)) {
                    ++j;
                }
            }

            SetBufferPosition(byteBuffer, i);
            this.charPosition = newCharPosition;

            return i;
        }
    }
}
