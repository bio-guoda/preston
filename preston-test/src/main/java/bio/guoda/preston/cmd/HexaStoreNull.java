package bio.guoda.preston.cmd;

import bio.guoda.preston.store.HexaStore;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.api.RDFTerm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class HexaStoreNull implements HexaStore {
    final AtomicInteger putLogVersionAttemptCount = new AtomicInteger(0);
    final List<Pair<Pair<RDFTerm, RDFTerm>, RDFTerm>> queryAndValuePairs = new ArrayList<>();

    @Override
    public void put(Pair<RDFTerm, RDFTerm> queryKey, RDFTerm value) throws IOException {
        putLogVersionAttemptCount.incrementAndGet();
        queryAndValuePairs.add(Pair.of(queryKey, value));
    }

    @Override
    public IRI get(Pair<RDFTerm, RDFTerm> queryKey) throws IOException {
        return null;
    }
}
