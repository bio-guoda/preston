package org.globalbioticinteractions.preston.store;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

public interface BlobStore {
    URI putBlob(InputStream is) throws IOException;

    URI putBlob(URI entity) throws IOException;

    InputStream get(URI key) throws IOException;


}
