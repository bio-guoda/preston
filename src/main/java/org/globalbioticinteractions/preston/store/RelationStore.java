package org.globalbioticinteractions.preston.store;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import java.io.IOException;
import java.net.URI;

public interface RelationStore {
    void put(Pair<URI, URI> unhashedKeyPair, String value) throws IOException;

    void put(Triple<URI, URI, URI> statement) throws IOException;

    String findKey(Pair<URI, URI> partialStatement) throws IOException;

}
