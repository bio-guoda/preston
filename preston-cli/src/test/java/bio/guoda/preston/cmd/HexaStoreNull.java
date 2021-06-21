package bio.guoda.preston.cmd;

import bio.guoda.preston.store.HexaStore;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDFTerm;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class HexaStoreNull implements HexaStore {
    final AtomicInteger putLogVersionAttemptCount = new AtomicInteger(0);

    @Override
    public void put(Pair<RDFTerm, RDFTerm> queryKey, RDFTerm value) throws IOException {
        putLogVersionAttemptCount.incrementAndGet();
    }

    @Override
    public IRI get(Pair<RDFTerm, RDFTerm> queryKey) throws IOException {
        return null;
    }
}
