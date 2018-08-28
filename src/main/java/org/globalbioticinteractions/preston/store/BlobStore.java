package org.globalbioticinteractions.preston.store;

import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

public interface BlobStore {
    String putBlob(InputStream is) throws IOException;

    String putBlob(URI entity) throws IOException;

    InputStream get(String key) throws IOException;


}
