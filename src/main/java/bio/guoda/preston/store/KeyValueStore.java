package bio.guoda.preston.store;

import java.io.IOException;
import java.io.InputStream;

public interface KeyValueStore extends KeyValueStoreReadOnly {
    void put(String key, String value) throws IOException;

    String put(KeyGeneratingStream keyGeneratingStream, InputStream is) throws IOException;

    void put(String key, InputStream is) throws IOException;

}
