package bio.guoda.preston.stream;

import bio.guoda.preston.process.StatementsEmitter;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.txt.UniversalEncodingDetector;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static bio.guoda.preston.RefNodeConstants.DESCRIPTION;
import static bio.guoda.preston.RefNodeConstants.HAD_MEMBER;
import static bio.guoda.preston.RefNodeConstants.HAS_VALUE;
import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.RefNodeFactory.toLiteral;
import static bio.guoda.preston.RefNodeFactory.toStatement;
import static bio.guoda.preston.stream.CharBufferByteReader.getBufferPosition;
import static bio.guoda.preston.stream.CharBufferByteReader.setBufferPosition;
import static bio.guoda.preston.stream.ContentStreamFactory.URI_PREFIX_CUT;

public class MatchingTextStreamHandler implements ContentStreamHandler {
    private static final int BUFFER_SIZE = 4096;
    private static final int MAX_MATCH_SIZE_IN_BYTES = 512;

    private final ContentStreamHandler contentStreamHandler;
    private final StatementsEmitter emitter;
    private final Pattern pattern;
    private final Map<Integer, String> patternGroupNames;
    private final boolean reportOnlyMatchingText;

    public MatchingTextStreamHandler(ContentStreamHandler contentStreamHandler, StatementsEmitter emitter, Pattern pattern, boolean reportOnlyMatchingText) {
        this.contentStreamHandler = contentStreamHandler;
        this.emitter = emitter;
        this.pattern = pattern;
        this.patternGroupNames = extractPatternGroupNames(pattern);
        this.reportOnlyMatchingText = reportOnlyMatchingText;
    }

    private static IRI getCutIri(IRI fileIri, int startAt, int endAt) {
        return toIRI(String.format((URI_PREFIX_CUT + ":") + "%s!/b%d-%d", fileIri.getIRIString(), startAt + 1, endAt));
    }

    @Override
    public boolean handle(IRI version, InputStream is) throws ContentStreamException {
        Charset charset;
        try {
            charset = new UniversalEncodingDetector().detect(is, new Metadata());
        } catch (IOException e) {
            throw new ContentStreamException("failed to detect charset", e);
        }
        if (charset != null) {
            try {
                findAndEmitTextMatches(version, is, charset);
            } catch (IOException e) {
                throw new ContentStreamException("failed to parse text", e);
            }
            return true;
        }
        return false;
    }

    @Override
    public boolean shouldKeepProcessing() {
        return contentStreamHandler.shouldKeepProcessing();
    }

    private void findAndEmitTextMatches(IRI version, InputStream is, Charset charset) throws IOException {
        if (reportOnlyMatchingText) {
            emitAllTextMatches(version, is, charset);
        } else {
            emitAnyTextMatch(version, is, charset);
        }
    }

    private void emitAnyTextMatch(IRI contentIri, InputStream is, Charset charset) throws IOException {
        ByteBuffer fullText = ByteBuffer.wrap(IOUtils.toByteArray(is));
        CharBuffer charBuffer = charset.decode(fullText);

        Matcher matcher = pattern.matcher(charBuffer);
        if (matcher.find()) {
            emitter.emit(Collections.singletonList(toStatement(contentIri, HAS_VALUE, toLiteral(charBuffer.toString()))));
        }
    }

    private void emitAllTextMatches(IRI version, InputStream is, Charset charset) throws IOException {
        byte[] byteBuffer = new byte[BUFFER_SIZE];

        int offset = 0;
        int numBytesToReuse = 0;
        int numBytesScannedInLastIteration = 0;
        while (contentStreamHandler.shouldKeepProcessing()) {
            int numBytesToScan = numBytesToReuse;

            // Copy text from the end of the buffer to the beginning in case
            // matches occur across buffer boundaries
            System.arraycopy(byteBuffer, numBytesScannedInLastIteration - numBytesToReuse, byteBuffer, 0, numBytesToReuse);

            int numBytesRead;
            while ((numBytesRead = is.read(byteBuffer, numBytesToScan, byteBuffer.length - numBytesToScan)) > 0) {
                numBytesToScan += numBytesRead;
            }

            if (numBytesToScan <= 0) {
                break;
            }

            ByteBuffer scanningByteBuffer = ByteBuffer.wrap(byteBuffer, 0, numBytesToScan);
            emitAllTextMatches(version, charset, offset, scanningByteBuffer);

            // If no matches were found, we need to advance the buffer's position manually
            if (getBufferPosition(scanningByteBuffer) == 0) {
                setBufferPosition(scanningByteBuffer, scanningByteBuffer.limit());
            }

            numBytesScannedInLastIteration = numBytesToScan;
            numBytesToReuse = Integer.min(MAX_MATCH_SIZE_IN_BYTES, numBytesScannedInLastIteration - getBufferPosition(scanningByteBuffer));
            offset += numBytesScannedInLastIteration - numBytesToReuse;
        }
    }

    private void emitAllTextMatches(IRI version, Charset charset, int offset, ByteBuffer byteBuffer) throws CharacterCodingException {
        // Default CharBuffer::decode behavior is to replace uninterpretable bytes with an "unknown" character that
        // is not always the same length in bytes. Instead, ignore those bytes and reinsert them after encoding.
        CharsetDecoder decoder = charset.newDecoder();
        CharBuffer charBuffer = decoder
                .onMalformedInput(CodingErrorAction.IGNORE)
                .onUnmappableCharacter(CodingErrorAction.IGNORE)
                .decode(byteBuffer);

        setBufferPosition(byteBuffer, 0);

        Matcher matcher = pattern.matcher(charBuffer);
        CharBufferByteReader charBufferByteReader = new CharBufferByteReader(byteBuffer, charBuffer, charset);

        while (contentStreamHandler.shouldKeepProcessing() && matcher.find()) {
            int bytePosMatchStartsAt = charBufferByteReader.advance(matcher.start());
            if (bytePosMatchStartsAt >= BUFFER_SIZE - MAX_MATCH_SIZE_IN_BYTES) {
                setBufferPosition(byteBuffer, bytePosMatchStartsAt);
                break;
            }

            emitMatches(version, offset, matcher, getCharToBytePositionsMap(matcher, charBufferByteReader));
        }
    }

    private void emitMatches(IRI version, int offset, Matcher matcher, Map<Integer, Integer> charToBytePositions) {
        // Because characters can have variable width, report byte positions instead of character positions
        int bytePosMatchStartsAt = offset + charToBytePositions.get(matcher.start());
        int bytePosMatchEndsAt = offset + charToBytePositions.get(matcher.end());

        IRI matchIri = getCutIri(version, bytePosMatchStartsAt, bytePosMatchEndsAt);

        List<Quad> statements = new LinkedList<>();
        for (int i = 0; i <= matcher.groupCount(); ++i) {
            if (matcher.group(i) != null) {
                int charPosGroupStartsAt = matcher.start(i);
                int charPosGroupEndsAt = matcher.end(i);

                int bytePosGroupStartsAt = charToBytePositions.get(charPosGroupStartsAt);
                int bytePosGroupEndsAt = charToBytePositions.get(charPosGroupEndsAt);

                String groupString = matcher.group(i);

                if (i == 0) {
                    statements.add(toStatement(matchIri, HAS_VALUE, toLiteral(groupString)));
                } else {
                    IRI groupIri = getCutIri(version, offset + bytePosGroupStartsAt, offset + bytePosGroupEndsAt);
                    statements.add(toStatement(groupIri, HAS_VALUE, toLiteral(groupString)));
                    statements.add(toStatement(matchIri, HAD_MEMBER, groupIri));

                    if (patternGroupNames.containsKey(i)) {
                        String groupName = patternGroupNames.get(i);
                        if (groupString.equals(matcher.group(groupName))) {
                            statements.add(toStatement(matchIri, DESCRIPTION, toLiteral(groupName)));
                        } else {
                            throw new RuntimeException("pattern group [" + groupName + "] was assigned the wrong index");
                        }
                    }
                }
            }
        }
        emitter.emit(statements);
    }

    private Map<Integer, Integer> getCharToBytePositionsMap(Matcher matcher, CharBufferByteReader charBufferByteReader) {
        List<Integer> orderedCharPositions = new LinkedList<>();
        for (int i = 0; i <= matcher.groupCount(); ++i) {
            if (matcher.group(i) != null) {
                orderedCharPositions.add(matcher.start(i));
                orderedCharPositions.add(matcher.end(i));
            }
        }
        orderedCharPositions.sort(null);

        return orderedCharPositions
                .stream()
                .distinct()
                .collect(Collectors.toMap(
                        charPosition -> charPosition,
                        charBufferByteReader::advance
                ));
    }

    public static Map<Integer, String> extractPatternGroupNames(Pattern pattern) {

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

    public static Pattern sterilizePatternForGroupDetection(Pattern pattern) {
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
        } else {
            return deepReplaceAll(newString, pattern, replacement);
        }
    }


}
