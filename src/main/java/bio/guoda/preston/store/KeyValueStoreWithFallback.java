package bio.guoda.preston.store;

import java.io.IOException;
import java.io.InputStream;

public class KeyValueStoreWithFallback implements KeyValueStore {
    private final KeyValueStore primary;
    private final KeyValueStoreReadOnly readOnlyFallback;

    public KeyValueStoreWithFallback(KeyValueStore primary, KeyValueStoreReadOnly readOnlyFallback) {
        this.primary = primary;
        this.readOnlyFallback = readOnlyFallback;
    }

    @Override
    public void put(String key, String value) throws IOException {
        primary.put(key, value);
    }

    @Override
    public String put(KeyGeneratingStream keyGeneratingStream, InputStream is) throws IOException {
        return primary.put(keyGeneratingStream, is);
    }

    @Override
    public void put(String key, InputStream is) throws IOException {
        primary.put(key, is);
    }

    @Override
    public InputStream get(String key) throws IOException {
        InputStream is = primary.get(key);
        if (is == null) {
            is = readOnlyFallback.get(key);
        }
        return is;
    }
}
