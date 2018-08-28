package org.globalbioticinteractions.preston.store;

import java.io.IOException;
import java.io.InputStream;

public interface Persistence {
    void put(String key, String value) throws IOException;

    String put(KeyGeneratingStream keyGeneratingStream, InputStream is) throws IOException;

    InputStream get(String key) throws IOException;
}
