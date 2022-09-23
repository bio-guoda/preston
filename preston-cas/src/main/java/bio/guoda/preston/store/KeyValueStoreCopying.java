package bio.guoda.preston.store;

import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;

public class KeyValueStoreCopying implements KeyValueStore {
    private final KeyValueStore targetKeyValueStore;
    private final KeyValueStoreReadOnly sourceKeyValueStore;

    public KeyValueStoreCopying(KeyValueStoreReadOnly source, KeyValueStore target) {
        this.sourceKeyValueStore = source;
        this.targetKeyValueStore = target;
    }

    @Override
    public IRI put(KeyGeneratingStream keyGeneratingStream, InputStream is) throws IOException {
        return targetKeyValueStore.put(keyGeneratingStream, is);
    }

    @Override
    public void put(IRI key, InputStream is) throws IOException {
        targetKeyValueStore.put(key, is);
    }

    @Override
    public InputStream get(IRI key) throws IOException {
        InputStream is = targetKeyValueStore.get(key);
        if (is == null) {
            is = sourceKeyValueStore.get(key);
            if (is != null) {
                targetKeyValueStore.put(key, is);
                is.close();
                is = targetKeyValueStore.get(key);
            }
        }
        return is;
    }
}
