package bio.guoda.preston.process;

import bio.guoda.preston.model.RefNodeFactory;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.cxf.common.util.URIParserUtil;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.txt.UniversalEncodingDetector;
import sun.nio.cs.ThreadLocalCoders;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CodingErrorAction;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
    public static final Pattern URL_PATTERN = Pattern.compile("(?>https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]");

    private final Pattern pattern;
    private int batchSize = 256;

    public TextMatcher(BlobStoreReadOnly blobStoreReadOnly, StatementsListener... listeners) {
        this(URL_PATTERN, blobStoreReadOnly, listeners);
    }

    public TextMatcher(String regex, BlobStoreReadOnly blobStoreReadOnly, StatementsListener... listeners) {
        this(Pattern.compile(regex), blobStoreReadOnly, listeners);
    }

    public TextMatcher(Pattern pattern, BlobStoreReadOnly blobStoreReadOnly, StatementsListener... listeners) {
        super(blobStoreReadOnly, listeners);
        this.pattern = pattern;
    }

    @Override
    public void on(Quad statement) {
        if (hasVersionAvailable(statement)) {
            IRI version = (IRI) getVersion(statement);
            final List<Quad> nodes = new ArrayList<>();

            BatchingEmitter batchingStatementEmitter = new BatchingEmitter(nodes, version, statement);
            try (InputStream in = get(version)) {
                if (in != null) {
                    InputStream markableInputStream = (in.markSupported()) ? in : new BufferedInputStream(in);
                    attemptToParse(version, markableInputStream, batchingStatementEmitter);
                }
            } catch (IOException e) {
                // ignore; this is opportunistic
            }

            // emit remaining
            batchingStatementEmitter.emitBatch();
        }
    }

    private boolean attemptToParse(IRI version, InputStream in, StatementEmitter emitter) throws IOException {
        return (attemptToParseAsArchive(version, in, emitter) ||
                attemptToParseAsCompressed(version, in, emitter) ||
                attemptToParseAsText(version, in, emitter));
    }

    private boolean attemptToParseAsArchive(IRI version, InputStream in, StatementEmitter emitter) throws IOException {
        ArchiveInputStream archiveStream = getArchiveStream(in);
        if (archiveStream != null) {
            parseAsArchive(version, archiveStream, emitter);
            return true;
        }
        return false;
    }

    private ArchiveInputStream getArchiveStream(InputStream in) {
        try {
            return new ArchiveStreamFactory()
                    .createArchiveInputStream(in);
        } catch (ArchiveException e) {
            return null;
        }
    }

    private boolean attemptToParseAsCompressed(IRI version, InputStream in, StatementEmitter emitter) throws IOException {
        InputStream compressedStream = getCompressedStream(in);
        if (compressedStream != null) {
            parseAsCompressed(version, compressedStream, emitter);
            return true;
        }
        return false;
    }

    private InputStream getCompressedStream(InputStream in) {
        try {
            return new CompressorStreamFactory()
                    .createCompressorInputStream(in);
        } catch (CompressorException e) {
            return null;
        }
    }

    private boolean attemptToParseAsText(IRI version, InputStream in, StatementEmitter emitter) throws IOException {
        Charset charset = new UniversalEncodingDetector().detect(in, new Metadata());
        if (charset != null) {
            parseAsText(version, in, emitter, charset);
            return true;
        }
        return false;
    }

    private void parseAsArchive(IRI version, ArchiveInputStream in, StatementEmitter emitter) throws IOException {
        ArchiveEntry entry;
        while ((entry = in.getNextEntry()) != null) {
            if (in.canReadEntryData(entry)) {
                InputStream entryStream = new BufferedInputStream(in);
                try {
                    attemptToParse(getEntryIri(version, entry.getName()), entryStream, emitter);
                } catch (IOException e) {
                    // ignore; this is opportunistic
                }
            }
        }
    }

    private void parseAsCompressed(IRI version, InputStream in, StatementEmitter emitter) throws IOException {
        attemptToParse(version, in, emitter);
    }

    private void parseAsText(IRI version, InputStream in, StatementEmitter emitter, Charset charset) throws IOException {
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

    private static IRI getEntryIri(IRI version, String name) {
        return toIRI(URIParserUtil.escapeChars((String.format("zip:%s!/%s", version.getIRIString(), name))));
    }

    private static IRI getLineIri(IRI fileIri, int line) {
        return toIRI(String.format("line:%s!/%d", fileIri.getIRIString(), line));
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

    private int advanceToCorrespondingByte(ByteBuffer byteBuffer, ByteBuffer filteredByteBuffer) {
        int i = GetBufferPosition(byteBuffer);
        for (int j = GetBufferPosition(filteredByteBuffer); i < byteBuffer.limit() && j < filteredByteBuffer.limit(); ++i) {
            if (byteBuffer.get(i) == filteredByteBuffer.get(j)) {
                ++j;
            }
        }

        SetBufferPosition(byteBuffer, i);
        return i;
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
