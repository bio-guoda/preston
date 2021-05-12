package bio.guoda.preston.stream;

import bio.guoda.preston.process.StatementEmitter;
import org.apache.commons.rdf.api.IRI;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.txt.UniversalEncodingDetector;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
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
import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static bio.guoda.preston.model.RefNodeFactory.toLiteral;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;
import static bio.guoda.preston.stream.CharBufferByteReader.getBufferPosition;
import static bio.guoda.preston.stream.CharBufferByteReader.setBufferPosition;

public class MatchingTextStreamHandler implements ContentStreamHandler {
    private static final int BUFFER_SIZE = 4096;
    private static final int MAX_MATCH_SIZE_IN_BYTES = 512;

    private final StatementEmitter emitter;
    private final Pattern pattern;
    private final Map<Integer, String> patternGroupNames;

    public MatchingTextStreamHandler(StatementEmitter emitter, Pattern pattern) {
        this.emitter = emitter;
        this.pattern = pattern;
        this.patternGroupNames = extractPatternGroupNames(pattern);
    }

    private static IRI getCutIri(IRI fileIri, int startAt, int endAt) {
        return toIRI(String.format("cut:%s!/b%d-%d", fileIri.getIRIString(), startAt + 1, endAt));
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
    public boolean shouldKeepReading() {
        return true;
    }

    private void findAndEmitTextMatches(IRI version, InputStream is, Charset charset) throws IOException {
        byte[] byteBuffer = new byte[BUFFER_SIZE];
        CharsetDecoder decoder = charset.newDecoder();

        int offset = 0;
        int numBytesToReuse = 0;
        int numBytesScannedInLastIteration = 0;
        while (true) {
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

            // Default CharBuffer::decode behavior is to replace uninterpretable bytes with an "unknown" character that
            // is not always the same length in bytes. Instead, ignore those bytes and reinsert them after encoding.
            CharBuffer charBuffer = decoder
                    .onMalformedInput(CodingErrorAction.IGNORE)
                    .onUnmappableCharacter(CodingErrorAction.IGNORE)
                    .decode(scanningByteBuffer);

            setBufferPosition(scanningByteBuffer, 0);

            findAndEmitMatches(version, charset, offset, scanningByteBuffer, charBuffer);

            // If no matches were found, we need to advance the buffer's position manually
            if (getBufferPosition(scanningByteBuffer) == 0) {
                setBufferPosition(scanningByteBuffer, scanningByteBuffer.limit());
            }

            numBytesScannedInLastIteration = numBytesToScan;
            numBytesToReuse = Integer.min(MAX_MATCH_SIZE_IN_BYTES, numBytesScannedInLastIteration - getBufferPosition(scanningByteBuffer));
            offset += numBytesScannedInLastIteration - numBytesToReuse;
        }
    }

    private void findAndEmitMatches(IRI version, Charset charset, int offset, ByteBuffer scanningByteBuffer, CharBuffer charBuffer) {
        Matcher matcher = pattern.matcher(charBuffer);
        CharBufferByteReader charBufferByteReader = new CharBufferByteReader(scanningByteBuffer, charBuffer, charset);

        while (matcher.find()) {
            int charPosMatchStartsAt = matcher.start();
            int bytePosMatchStartsAt = charBufferByteReader.advance(charPosMatchStartsAt);

            if (bytePosMatchStartsAt >= BUFFER_SIZE - MAX_MATCH_SIZE_IN_BYTES) {
                setBufferPosition(scanningByteBuffer, bytePosMatchStartsAt);
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

            // Because characters can have variable width,
            // report byte positions instead of character positions
            Map<Integer, Integer> charToBytePositions = orderedCharPositions
                    .stream()
                    .distinct()
                    .collect(Collectors.toMap(
                            charPosition -> charPosition,
                            charBufferByteReader::advance
                    ));

            int charPosMatchEndsAt = matcher.end();
            int bytePosMatchEndsAt = charToBytePositions.get(charPosMatchEndsAt);
            IRI matchIri = getCutIri(version, offset + bytePosMatchStartsAt, offset + bytePosMatchEndsAt);

            emitMatches(version, offset, matcher, charToBytePositions, matchIri);
        }
    }

    private void emitMatches(IRI version, int offset, Matcher matcher, Map<Integer, Integer> charToBytePositions, IRI matchIri) {
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
                        } else {
                            throw new RuntimeException("pattern group [" + groupName + "] was assigned the wrong index");
                        }
                    }
                }
            }
        }
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
