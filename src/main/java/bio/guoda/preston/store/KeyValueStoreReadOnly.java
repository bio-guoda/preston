package bio.guoda.preston.store;

import java.io.IOException;
import java.io.InputStream;

public interface KeyValueStoreReadOnly {
    InputStream get(String key) throws IOException;
}
