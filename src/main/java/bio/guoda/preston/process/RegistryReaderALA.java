package bio.guoda.preston.process;

import org.apache.commons.rdf.api.Triple;

public class RegistryReaderALA extends ProcessorReadOnly {
    public RegistryReaderALA(BlobStoreReadOnly testBlobStore, StatementListener listener) {
        super(testBlobStore, listener);

    }

    @Override
    public void on(Triple statement) {

    }
}
