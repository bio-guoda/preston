package org.globalbioticinteractions.preston.process;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Objects;

public abstract class ProcessorReadOnly extends RefStatementProcessor {

    private final BlobStoreReadOnly blobStoreReadOnly;

    public ProcessorReadOnly(BlobStoreReadOnly blobStoreReadOnly, RefStatementListener... listeners) {
        super(listeners);
        Objects.requireNonNull(blobStoreReadOnly);
        this.blobStoreReadOnly = blobStoreReadOnly;
    }

    protected InputStream get(URI uri) throws IOException {
        return blobStoreReadOnly.get(uri);
    }


}
