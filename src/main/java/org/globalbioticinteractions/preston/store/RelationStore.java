package org.globalbioticinteractions.preston.store;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import java.io.IOException;

public interface RelationStore<T> {
    void put(Pair<T, T> partialStatement, String value) throws IOException;

    void put(Triple<T, T, T> statement) throws IOException;

    String findKey(Pair<T, T> partialStatement) throws IOException;

}
