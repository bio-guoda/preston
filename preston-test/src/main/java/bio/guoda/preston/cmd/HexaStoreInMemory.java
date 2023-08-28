package bio.guoda.preston.cmd;

import bio.guoda.preston.RefNodeFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDFTerm;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class HexaStoreInMemory extends HexaStoreNull {
    private final Map<Pair<String, String>, String> hexaStore = new TreeMap<>();

    @Override
    public void put(Pair<RDFTerm, RDFTerm> queryKey, RDFTerm value) throws IOException {
        super.put(queryKey, value);
        Pair<String, String> iriPair = iriPairOf(queryKey);
        hexaStore.put(iriPair, termToIRIString(value));
    }

    private Pair<String, String> iriPairOf(Pair<RDFTerm, RDFTerm> queryKey) {
        return Pair.of(
                termToIRIString(queryKey.getKey()),
                termToIRIString(queryKey.getValue())
        );
    }

    private String termToIRIString(RDFTerm key) {
        String str = key.ntriplesString();
        return StringUtils.substring(str, 1, str.length() - 1);
    }

    @Override
    public IRI get(Pair<RDFTerm, RDFTerm> queryKey) throws IOException {
        String rdfTerm = hexaStore.get(iriPairOf(queryKey));
        return rdfTerm == null ? null : RefNodeFactory.toIRI(rdfTerm);
    }
}
