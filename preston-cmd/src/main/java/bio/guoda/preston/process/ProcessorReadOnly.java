package bio.guoda.preston.process;

import bio.guoda.preston.store.Dereferencer;
import bio.guoda.preston.store.KeyValueStoreReadOnly;
import bio.guoda.preston.store.StatementProcessor;
import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

public abstract class ProcessorReadOnly extends StatementProcessor implements KeyValueStoreReadOnly {

    private final Dereferencer<InputStream> blobStoreReadOnly;

    public ProcessorReadOnly(Dereferencer<InputStream> blobStoreReadOnly, StatementsListener... listeners) {
        this(blobStoreReadOnly, new ProcessorStateAlwaysContinue(), listeners);
    }
    public ProcessorReadOnly(Dereferencer<InputStream> blobStoreReadOnly, ProcessorState state, StatementsListener... listeners) {
        super(state, listeners);
        Objects.requireNonNull(blobStoreReadOnly);
        this.blobStoreReadOnly = blobStoreReadOnly;
    }

    @Override
    public InputStream get(IRI uri) throws IOException {
        return blobStoreReadOnly.get(uri);
    }



}
