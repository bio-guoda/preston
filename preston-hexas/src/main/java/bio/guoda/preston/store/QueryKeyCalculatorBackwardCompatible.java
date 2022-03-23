package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import bio.guoda.preston.RefNodeConstants;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDFTerm;

public class QueryKeyCalculatorBackwardCompatible extends QueryKeyCalculatorImpl {

    public QueryKeyCalculatorBackwardCompatible(HashType type) {
        super(type);
    }

    @Override
    public IRI calculateKeyFor(Pair<RDFTerm, RDFTerm> unhashedKeyPair) {
        IRI queryKey;
        if (RefNodeConstants.BIODIVERSITY_DATASET_GRAPH_URN_UUID.equals(unhashedKeyPair.getLeft())
                && RefNodeConstants.HAS_VERSION.equals(unhashedKeyPair.getRight())) {
            queryKey = RefNodeConstants.PROVENANCE_ROOT_QUERY_HASH_URI;
        } else {
            queryKey = super.calculateKeyFor(unhashedKeyPair);
        }
        return queryKey;
    }

}
