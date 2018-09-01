package org.globalbioticinteractions.preston.store;

import org.globalbioticinteractions.preston.process.BlobStoreReadOnly;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

public interface BlobStore extends BlobStoreReadOnly {

    URI putBlob(InputStream is) throws IOException;

    URI putBlob(URI entity) throws IOException;

}
