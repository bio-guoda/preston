package org.globalbioticinteractions.preston.process;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

public interface BlobStoreReadOnly {
    InputStream get(URI key) throws IOException;
}
