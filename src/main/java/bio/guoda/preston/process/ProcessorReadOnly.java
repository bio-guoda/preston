package bio.guoda.preston.process;

import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

public abstract class ProcessorReadOnly extends StatementProcessor {

    private final BlobStoreReadOnly blobStoreReadOnly;

    public ProcessorReadOnly(BlobStoreReadOnly blobStoreReadOnly, StatementListener... listeners) {
        super(listeners);
        Objects.requireNonNull(blobStoreReadOnly);
        this.blobStoreReadOnly = blobStoreReadOnly;
    }

    protected InputStream get(IRI uri) throws IOException {
        return blobStoreReadOnly.get(uri);
    }



}
