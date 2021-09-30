package bio.guoda.preston.stream;

import bio.guoda.preston.DerefProgressListener;
import bio.guoda.preston.DerefState;
import bio.guoda.preston.store.ValidatingKeyValueStreamSHA256IRI;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.commons.io.input.CountingInputStream;
import org.apache.commons.rdf.api.IRI;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static bio.guoda.preston.RefNodeFactory.toIRI;

public class ContentStreamUtil {

    /**
     * @param in a stream of bytes.
     * @param startOffset the offset to start reading bytes from {@code in}.
     * @param endOffset the offset to stop reading at (non-inclusive).
     * @return a byte stream of length {@code (endOffset - startOffset)}.
     * @throws IOException if {@code in} can't be read up to {@code startOffset}.
     */
    public static InputStream cutBytes(InputStream in, long startOffset, long endOffset) throws IOException {
        IOUtils.skipFully(in, startOffset);
        return new BoundedInputStream(in, (endOffset - startOffset));
    }

    public static InputStream getInputStreamWithProgressLogger(IRI dataURI, DerefProgressListener listener, long contentLength, InputStream contentStream) {
        listener.onProgress(dataURI, DerefState.START, 0, contentLength);

        return new CountingInputStream(contentStream) {
            AtomicBoolean isDone = new AtomicBoolean(false);

            @Override
            public synchronized long skip(long length) throws IOException {
                long skip = super.skip(length);
                listener.onProgress(dataURI, DerefState.BUSY, getByteCount(), contentLength);
                return skip;
            }

            @Override
            protected synchronized void afterRead(int n) {
                super.afterRead(n);
                listener.onProgress(dataURI, DerefState.BUSY, getByteCount(), contentLength);
            }

            @Override
            public void close() throws IOException {
                super.close();
                if (!isDone.get()) {
                    listener.onProgress(dataURI, DerefState.DONE, getByteCount(), contentLength);
                    isDone.set(true);
                }
            }
        };
    }

    public static DerefProgressListener getNullDerefProgressListener() {
        return (dataURI, derefState, read, total) -> {};
    }

    public static IRI extractContentHash(IRI iri) throws IllegalArgumentException {
        final Pattern contentHashPattern = ValidatingKeyValueStreamSHA256IRI.URI_PATTERN_HASH_URI_SHA_256_PATTERN;
        Matcher contentHashMatcher = contentHashPattern.matcher(iri.getIRIString());

        IRI contentHash = (contentHashMatcher.find())
                ? toIRI(contentHashMatcher.group())
                : null;
        if (contentHash == null) {
            throw new IllegalArgumentException("[" + iri.getIRIString() + "] is not a content-based URI (e.g. \"...hash://sha256/abc123...\"");
        } else {
            return contentHash;
        }
    }

    public static InputStream getMarkSupportedInputStream(InputStream in) {
        return (in.markSupported()) ? in : new BufferedInputStream(in);
    }
}
