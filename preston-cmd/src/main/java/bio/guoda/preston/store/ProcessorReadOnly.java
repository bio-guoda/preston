package bio.guoda.preston.store;

import bio.guoda.preston.process.ProcessorState;
import bio.guoda.preston.process.StatementsListener;
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
