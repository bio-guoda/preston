package bio.guoda.preston.stream;

import bio.guoda.preston.DerefProgressListener;
import bio.guoda.preston.DerefState;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.ValidatingKeyValueStreamSHA256IRI;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.commons.io.input.CountingInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static bio.guoda.preston.RefNodeFactory.toIRI;

public class ContentStreamUtil {

    public static final Pattern GZ_APACHE_VFS_PATTERN = Pattern.compile("(.*:)(gz:)([^!]*)(!/[^!]*)(!/.*)+");


    public static final DerefProgressListener NOOP_DEREF_PROGRESS_LISTENER = (dataURI, derefState, read, total) -> {
    };

    /**
     * @param in          a stream of bytes.
     * @param startOffset the offset to start reading bytes from {@code in}.
     * @param endOffset   the offset to stop reading at (non-inclusive).
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

    public static DerefProgressListener getNOOPDerefProgressListener() {
        return NOOP_DEREF_PROGRESS_LISTENER;
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

    public static IRI truncateGZNotationForVFSIfNeeded(IRI iri) {
        return RefNodeFactory.toIRI(truncateGZNotationForVFSIfNeeded(iri.getIRIString()));
    }

    public static String truncateGZNotationForVFSIfNeeded(String url) {
        Matcher matcher = GZ_APACHE_VFS_PATTERN
                .matcher(url);

        if (matcher.matches() &&
                StringUtils.split(matcher.group(1), ":").length == StringUtils.split(matcher.group(5), "!/").length) {
            url = matcher.group(1) + matcher.group(2) + matcher.group(3) + matcher.group(5);
        }
        return url;
    }
}
