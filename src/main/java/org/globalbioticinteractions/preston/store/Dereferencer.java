package org.globalbioticinteractions.preston.store;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

public interface Dereferencer {
    InputStream dereference(URI uri) throws IOException;
}
