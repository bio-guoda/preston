package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import bio.guoda.preston.RefNodeConstants;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDFTerm;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

public class QueryKeyCalculatorBackwardCompatible extends QueryKeyCalculatorImpl {

    private static final Map<HashType, IRI> typeToRoot = Collections.unmodifiableMap(new TreeMap<HashType, IRI>() {
        {
            put(HashType.sha256, RefNodeConstants.PROVENANCE_ROOT_QUERY_HASH_SHA256_URI);
            put(HashType.md5, RefNodeConstants.PROVENANCE_ROOT_QUERY_HASH_MD5_URI);
        }

    });

    public QueryKeyCalculatorBackwardCompatible(HashType type) {
        super(type);
    }

    @Override
    public IRI calculateKeyFor(Pair<RDFTerm, RDFTerm> unhashedKeyPair) {
        IRI queryKey;
        if (RefNodeConstants.BIODIVERSITY_DATASET_GRAPH_URN_UUID.equals(unhashedKeyPair.getLeft())
                && RefNodeConstants.HAS_VERSION.equals(unhashedKeyPair.getRight())) {
            queryKey = typeToRoot.get(getHashType());
            if (queryKey == null) {
                throw new RuntimeException("unknown root key for [" + getHashType() + "], cannot retrieve version history");
            }
        } else {
            queryKey = super.calculateKeyFor(unhashedKeyPair);
        }
        return queryKey;
    }

}
