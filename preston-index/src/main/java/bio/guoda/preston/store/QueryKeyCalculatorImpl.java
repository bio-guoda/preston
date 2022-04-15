package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import bio.guoda.preston.Hasher;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDFTerm;

public class QueryKeyCalculatorImpl implements QueryKeyCalculator {

    private final HashType hashType;


    public QueryKeyCalculatorImpl(HashType hashType) {
        this.hashType = hashType;
    }

    @Override
    public IRI calculateKeyFor(Pair<RDFTerm, RDFTerm> unhashedKeyPair) {
        IRI left = HexaStoreImpl.calculateHashFor(unhashedKeyPair.getLeft(), hashType);
        IRI right = HexaStoreImpl.calculateHashFor(unhashedKeyPair.getRight(), hashType);
        return Hasher.calcHashIRI(
                left.getIRIString() + right.getIRIString(),
                hashType);
    }

    public HashType getHashType() {
        return hashType;
    }

}
