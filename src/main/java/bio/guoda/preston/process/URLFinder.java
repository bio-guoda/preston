package bio.guoda.preston.process;

import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.txt.UniversalEncodingDetector;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
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
import static bio.guoda.preston.model.RefNodeFactory.toLiteral;
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
        Charset charset = new UniversalEncodingDetector().detect(in, new Metadata());
        if (charset != null) {
            parseAsText(version, in, emitter, charset);
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

    private void parseAsText(IRI version, InputStream in, StatementEmitter emitter, Charset charset) throws IOException {
        byte[] byteBuffer = new byte[BUFFER_SIZE];

        int offset = 0;
        int numBytesToReuse = 0;
        while (true) {
            // Copy text from the end of the buffer to the beginning in case matches occur across buffer boundaries
            System.arraycopy(byteBuffer, byteBuffer.length - numBytesToReuse, byteBuffer, 0, numBytesToReuse);

            int numBytesRead = in.read(byteBuffer, numBytesToReuse, byteBuffer.length - numBytesToReuse);
            if (numBytesRead == -1) {
                break;
            }

            int numBytesToScan = numBytesToReuse + numBytesRead;
            CharBuffer charBuffer = charset.decode(ByteBuffer.wrap(byteBuffer, 0, numBytesToScan));
            Matcher matcher = URL_PATTERN.matcher(charBuffer);
            int nextBytePosition = 0;
            while (matcher.find()) {
                int charPosMatchStartsAt = matcher.start();
                int charPosMatchEndsAt = matcher.end();

                // Because UTF-8 characters have variable width, report byte positions instead of character positions
                int bytePosMatchStartsAt = offset + getNumBytesInCharBuffer(charset, charBuffer, 0, charPosMatchStartsAt);
                int matchSizeInBytes = getNumBytesInCharBuffer(charset, charBuffer, charPosMatchStartsAt, charPosMatchEndsAt);
                int bytePosMatchEndsAt = bytePosMatchStartsAt + matchSizeInBytes;

                String matchString = matcher.group();
                emitter.emit(toStatement(getCutIri(version, bytePosMatchStartsAt, bytePosMatchEndsAt), HAS_VALUE, toLiteral(matchString)));

                nextBytePosition = bytePosMatchEndsAt;
            }

            numBytesToReuse = Integer.min(MAX_MATCH_SIZE_IN_BYTES, byteBuffer.length - (nextBytePosition - offset));
            offset += byteBuffer.length - numBytesToReuse;
        }
    }

    private int getNumBytesInCharBuffer(Charset charset, CharBuffer charBuffer, int startAt, int endAt) {
        return charset.encode(charBuffer.subSequence(startAt, endAt)).limit();
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
