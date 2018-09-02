package org.globalbioticinteractions.preston.store;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDFTerm;
import org.apache.commons.rdf.api.Triple;

import java.io.IOException;

public interface StatementStore {
    void put(Pair<RDFTerm, RDFTerm> partialStatement, RDFTerm value) throws IOException;

    void put(Triple statement) throws IOException;

    IRI findKey(Pair<RDFTerm, RDFTerm> partialStatement) throws IOException;

}
