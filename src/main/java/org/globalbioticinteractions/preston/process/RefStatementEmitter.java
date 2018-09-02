package org.globalbioticinteractions.preston.process;


import org.apache.commons.rdf.api.Triple;

public interface RefStatementEmitter {
    void emit(Triple statement);
}
