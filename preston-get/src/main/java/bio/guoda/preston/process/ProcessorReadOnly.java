package bio.guoda.preston.process;

import bio.guoda.preston.cmd.ProcessorStateAlwaysContinue;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.KeyValueStoreReadOnly;
import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

public abstract class ProcessorReadOnly extends StatementProcessor implements KeyValueStoreReadOnly {

    private final KeyValueStoreReadOnly blobStoreReadOnly;

    public ProcessorReadOnly(KeyValueStoreReadOnly blobStoreReadOnly, StatementsListener... listeners) {
        this(blobStoreReadOnly, new ProcessorStateAlwaysContinue(), listeners);
    }
    public ProcessorReadOnly(KeyValueStoreReadOnly blobStoreReadOnly, ProcessorState state, StatementsListener... listeners) {
        super(state, listeners);
        Objects.requireNonNull(blobStoreReadOnly);
        this.blobStoreReadOnly = blobStoreReadOnly;
    }

    @Override
    public InputStream get(IRI uri) throws IOException {
        return blobStoreReadOnly.get(uri);
    }



}
