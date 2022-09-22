package bio.guoda.preston.store;

import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;

public class KeyValueStoreWithReadFallback implements KeyValueStore {
    private final KeyValueStore primary;
    private final KeyValueStoreReadOnly readOnlyFallback;

    public KeyValueStoreWithReadFallback(KeyValueStore primary, KeyValueStoreReadOnly readOnlyFallback) {
        this.primary = primary;
        this.readOnlyFallback = readOnlyFallback;
    }

    @Override
    public IRI put(KeyGeneratingStream keyGeneratingStream, InputStream is) throws IOException {
        return primary.put(keyGeneratingStream, is);
    }

    @Override
    public void put(IRI key, InputStream is) throws IOException {
        primary.put(key, is);
    }

    @Override
    public InputStream get(IRI key) throws IOException {
        InputStream is;

        try {
            is = primary.get(key);
        } catch (IOException e) {
            is = readOnlyFallback.get(key);
        }

        if (is == null) {
            throw new IOException("no results found for [" + key.getIRIString() + "]");
        }

        return is;
    }
}
