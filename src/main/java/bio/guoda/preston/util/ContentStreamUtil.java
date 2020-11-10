package bio.guoda.preston.util;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BoundedInputStream;

import java.io.IOException;
import java.io.InputStream;

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
}
