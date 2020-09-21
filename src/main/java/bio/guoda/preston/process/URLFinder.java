package bio.guoda.preston.process;

import bio.guoda.preston.RefNodeConstants;
import com.drew.lang.Charsets;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.tika.detect.TextDetector;
import org.apache.tika.mime.MediaType;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static bio.guoda.preston.RefNodeConstants.HAS_VALUE;
import static bio.guoda.preston.model.RefNodeFactory.getVersion;
import static bio.guoda.preston.model.RefNodeFactory.hasVersionAvailable;
import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;
import static bio.guoda.preston.process.ActivityUtil.emitAsNewActivity;

public class URLFinder extends ProcessorReadOnly {

    private static final int BUFFER_SIZE = 4096;
    private static final int MAX_MATCH_SIZE_IN_BYTES = 512;

    // From https://urlregex.com/
    private static final Pattern URL_PATTERN = Pattern.compile("(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]");

    public URLFinder(BlobStoreReadOnly blobStoreReadOnly, StatementsListener... listeners) {
        super(blobStoreReadOnly, listeners);
    }

    @Override
    public void on(Quad statement) {
        if (hasVersionAvailable(statement)) {
            IRI version = (IRI) getVersion(statement);
            List<Quad> nodes = new ArrayList<>();
            try (InputStream in = get(version)) {
                attemptToParse(version, in, new StatementsEmitterAdapter() {
                    @Override
                    public void emit(Quad statement) {
                        nodes.add(statement);
                    }
                });
            } catch (IOException e) {
                // ignore; this is opportunistic
            }
            emitAsNewActivity(nodes.stream(), this, statement.getGraphName());
        }
    }

    private boolean attemptToParse(IRI version, InputStream in, StatementEmitter emitter) throws IOException {
        return (attemptToParseAsZip(version, in, emitter) ||
                attemptToParseAsText(version, in, emitter));
    }

    private boolean attemptToParseAsZip(IRI version, InputStream in, StatementEmitter emitter) throws IOException {
        if (isStreamZipped(in)) {
            parseAsZip(version, in, emitter);
            return true;
        }
        return false;
    }

    private boolean attemptToParseAsText(IRI version, InputStream in, StatementEmitter emitter) throws IOException {
        if (isStreamPlainText(in)) {
            parseAsText(version, in, emitter);
            return true;
        }
        return false;
    }

    private void parseAsZip(IRI version, InputStream in, StatementEmitter emitter) throws IOException {
        ZipInputStream zIn = new ZipInputStream(in);

        ZipEntry entry;
        while ((entry = zIn.getNextEntry()) != null) {
            InputStream entryStream = new BufferedInputStream(zIn);
            try {
                attemptToParse(getEntryIri(version, entry.getName()), entryStream, emitter);
            } catch (IOException e) {
                // ignore; this is opportunistic
            }
        }
    }

    private void parseAsText(IRI version, InputStream in, StatementEmitter emitter) throws IOException {
        byte[] byteBuffer = new byte[BUFFER_SIZE];

        int offset = 0;
        int overlapSize = 0;
        while (true) {
            // Copy text from the end of the buffer to the beginning in case matches occur across buffer boundaries
            System.arraycopy(byteBuffer, byteBuffer.length - overlapSize, byteBuffer, 0, overlapSize);

            int numBytesRead = in.read(byteBuffer, overlapSize, byteBuffer.length - overlapSize);
            if (numBytesRead == -1) {
                break;
            }

            int numBytesToScan = overlapSize + numBytesRead;
            CharBuffer charBuffer = Charsets.UTF_8.decode(ByteBuffer.wrap(byteBuffer, 0, numBytesToScan));
            Matcher matcher = URL_PATTERN.matcher(charBuffer);
            int nextBytePosition = 0;
            while (matcher.find()) {
                int charPosMatchStartsAt = matcher.start();
                int charPosMatchEndsAt = matcher.end();

                // Because UTF-8 characters have variable width, report byte positions instead of character positions
                int bytePosMatchStartsAt = offset + charBuffer.subSequence(0, charPosMatchStartsAt).toString().getBytes().length;
                int matchSizeInBytes = charBuffer.subSequence(charPosMatchStartsAt, charPosMatchEndsAt).toString().getBytes().length;
                int bytePosMatchEndsAt = bytePosMatchStartsAt + matchSizeInBytes;

                String matchString = matcher.group();
                emitter.emit(toStatement(getCutIri(version, bytePosMatchStartsAt, bytePosMatchEndsAt), HAS_VALUE, toIRI(matchString)));

                nextBytePosition = bytePosMatchEndsAt;
            }

            overlapSize = Integer.min(MAX_MATCH_SIZE_IN_BYTES, byteBuffer.length - (nextBytePosition - offset));
            offset += byteBuffer.length - overlapSize;
        }
    }

    private boolean isStreamPlainText(InputStream stream) throws IOException {
        return (new TextDetector().detect(stream, null) == MediaType.TEXT_PLAIN);
    }

    private boolean isStreamZipped(InputStream stream) throws IOException {
        stream.mark(128);
        boolean isZip = (new ZipInputStream(stream).getNextEntry() != null);
        stream.reset();
        return isZip;
    }

    private static IRI getEntryIri(IRI version, String name) {
        return toIRI(String.format("zip:%s!/%s", version.getIRIString(), name));
    }

    private static IRI getLineIri(IRI fileIri, int line) {
        return toIRI(String.format("line:%s!/%d", fileIri.getIRIString(), line));
    }

    private static IRI getCutIri(IRI fileIri, int startAt, int endAt) {
        return toIRI(String.format("cut:%s!/b%d-%d", fileIri.getIRIString(), startAt + 1, endAt));
    }

}
