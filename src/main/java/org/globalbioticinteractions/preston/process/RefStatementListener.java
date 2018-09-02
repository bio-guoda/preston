package org.globalbioticinteractions.preston.process;

import org.apache.commons.rdf.api.Triple;

public interface RefStatementListener {
    void on(Triple statement);
}
