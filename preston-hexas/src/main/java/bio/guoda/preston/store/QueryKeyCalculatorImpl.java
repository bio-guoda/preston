package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import bio.guoda.preston.Hasher;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDFTerm;

public class QueryKeyCalculatorImpl implements QueryKeyCalculator {

    private final HashType type;

    public QueryKeyCalculatorImpl(HashType type) {
        this.type = type;
    }

    @Override
    public IRI calculateKeyFor(Pair<RDFTerm, RDFTerm> unhashedKeyPair) {
        IRI left = HexaStoreImpl.calculateHashFor(unhashedKeyPair.getLeft(), type);
        IRI right = HexaStoreImpl.calculateHashFor(unhashedKeyPair.getRight(), type);
        return Hasher.calcHashIRI(
                left.getIRIString() + right.getIRIString(),
                type);
    }
}
