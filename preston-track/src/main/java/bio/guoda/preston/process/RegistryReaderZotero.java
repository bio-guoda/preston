package bio.guoda.preston.process;

import bio.guoda.preston.store.BlobStoreReadOnly;
import org.apache.commons.rdf.api.Quad;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegistryReaderZotero extends ProcessorReadOnly {
    private static final Logger LOG = LoggerFactory.getLogger(RegistryReaderZotero.class);

    public RegistryReaderZotero(BlobStoreReadOnly blobStoreReadOnly, StatementsListener listener) {
        super(blobStoreReadOnly, listener);
    }

    @Override
    public void on(Quad statement) {
    }

}
