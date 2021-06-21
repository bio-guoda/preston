package bio.guoda.preston.store;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDFTerm;

interface QueryKeyCalculator {
    IRI calculateKeyFor(Pair<RDFTerm, RDFTerm> unhashedKeyPair);
}
