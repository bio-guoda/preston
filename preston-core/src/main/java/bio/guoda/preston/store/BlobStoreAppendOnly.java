package bio.guoda.preston.store;

import bio.guoda.preston.Hasher;
import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;


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
    public IRI put(InputStream is) throws IOException {
        return keyValueStore.put(
                (is1, os1) -> Hasher.calcSHA256(is1, os1, shouldCloseInputStream), is);
    }

    @Override
    public InputStream get(IRI key) throws IOException {
        return key == null ? null : keyValueStore.get(key);
    }

}
