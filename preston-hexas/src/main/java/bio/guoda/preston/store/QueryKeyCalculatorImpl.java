package bio.guoda.preston.store;

import bio.guoda.preston.Hasher;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDFTerm;

public class QueryKeyCalculatorImpl implements QueryKeyCalculator {

    @Override
    public IRI calculateKeyFor(Pair<RDFTerm, RDFTerm> unhashedKeyPair) {
        IRI left = HexaStoreImpl.calculateHashFor(unhashedKeyPair.getLeft());
        IRI right = HexaStoreImpl.calculateHashFor(unhashedKeyPair.getRight());
        return Hasher.calcSHA256(left.getIRIString() + right.getIRIString());
    }
}
