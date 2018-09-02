package org.globalbioticinteractions.preston.store;

import org.apache.commons.rdf.api.BlankNode;
import org.apache.commons.rdf.api.Triple;

import java.io.IOException;

public interface VersionHandler {

    void handle(Triple statement, BlankNode version) throws IOException;
}
