package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import bio.guoda.preston.Hasher;
import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;


public class BlobStoreAppendOnly implements BlobStore {

    private final KeyValueStore keyValueStore;
    private final boolean shouldCloseInputStream;
    private final HashType type;

    public BlobStoreAppendOnly(KeyValueStore keyValueStore, boolean shouldCloseInputStream, HashType type) {
        this.keyValueStore = keyValueStore;
        this.shouldCloseInputStream = shouldCloseInputStream;
        this.type = type;
    }

    // write-once, read-many
    @Override
    public IRI put(InputStream is) throws IOException {
        return getKeyValueStore().put(
                (is1, os1) -> Hasher.calcHashIRI(is1, os1, shouldCloseInputStream(), getType()), is);
    }

    public KeyValueStore getKeyValueStore() {
        return keyValueStore;
    }

    public HashType getType() {
        return type;
    }

    public boolean shouldCloseInputStream() {
        return shouldCloseInputStream;
    }

    @Override
    public InputStream get(IRI key) throws IOException {
        return key == null ? null : getKeyValueStore().get(key);
    }

}
