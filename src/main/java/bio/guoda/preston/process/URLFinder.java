package bio.guoda.preston.process;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
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
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static bio.guoda.preston.RefNodeConstants.HAS_VALUE;
import static bio.guoda.preston.RefNodeConstants.USED;
import static bio.guoda.preston.model.RefNodeFactory.getVersion;
import static bio.guoda.preston.model.RefNodeFactory.hasVersionAvailable;
import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static bio.guoda.preston.model.RefNodeFactory.toLiteral;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;
import static bio.guoda.preston.process.ActivityUtil.emitAsNewActivity;

public class URLFinder extends ProcessorReadOnly {

    private static final int BUFFER_SIZE = 40096;
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

            BlankNodeOrIRI newActivity = toIRI(UUID.randomUUID());
            List<Quad> nodes = new ArrayList<>();
            nodes.add(toStatement(newActivity, USED, version));

            try (InputStream in = get(version)) {
                if (in != null) {
                    attemptToParse(version, in, new StatementsEmitterAdapter() {
                        @Override
                        public void emit(Quad statement) {
                            nodes.add(statement);
                        }
                    });
                }
            } catch (IOException e) {
                // ignore; this is opportunistic
            }

            emitAsNewActivity(nodes.stream(), this, statement.getGraphName(), newActivity);
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
            // Copy text from the end of the buffer to the beginning in case matches occur across buffer boundaries
            System.arraycopy(byteBuffer, numBytesScannedInLastIteration - numBytesToReuse, byteBuffer, 0, numBytesToReuse);

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

            numBytesScannedInLastIteration = numBytesToScan;
            numBytesToReuse = Integer.min(MAX_MATCH_SIZE_IN_BYTES, numBytesScannedInLastIteration - (nextBytePosition - offset));
            offset += numBytesScannedInLastIteration - numBytesToReuse;
        }
    }

    private int getNumBytesInCharBuffer(Charset charset, CharBuffer charBuffer, int startAt, int endAt) {
        return charset.encode(charBuffer.subSequence(startAt, endAt)).limit();
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
