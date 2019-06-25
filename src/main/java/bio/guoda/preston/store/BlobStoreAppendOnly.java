package bio.guoda.preston.store;

import bio.guoda.preston.Hasher;
import bio.guoda.preston.RDFUtil;
import bio.guoda.preston.model.RefNodeFactory;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDFTerm;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;


public class BlobStoreAppendOnly implements BlobStore {

    private final KeyValueStore keyValueStore;
    private final boolean shouldCloseInputStream;

    public BlobStoreAppendOnly(KeyValueStore keyValueStore) {
        this(keyValueStore, true);
    }

    public BlobStoreAppendOnly(KeyValueStore keyValueStore, boolean shouldCloseInputStream) {
        this.keyValueStore = keyValueStore;
        this.shouldCloseInputStream = shouldCloseInputStream;
    }

    // write-once, read-many
    @Override
    public IRI putBlob(InputStream is) throws IOException {
        return RefNodeFactory.toIRI(URI.create(keyValueStore.put((is1, os1) -> {
            IRI key = Hasher.calcSHA256(is1, os1, shouldCloseInputStream);
            return key.getIRIString();
        }, is)));
    }

    @Override
    public IRI putBlob(RDFTerm entity) throws IOException {
        String value = RDFUtil.getValueFor(entity);
        try (InputStream is = IOUtils.toInputStream(value, StandardCharsets.UTF_8)) {
            return putBlob(is);
        }
    }

    @Override
    public InputStream get(IRI key) throws IOException {
        return key == null ? null : keyValueStore.get(key.getIRIString());
    }

}
