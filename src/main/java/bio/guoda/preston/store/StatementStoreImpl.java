package bio.guoda.preston.store;

import bio.guoda.preston.Hasher;
import bio.guoda.preston.RDFUtil;
import bio.guoda.preston.model.RefNodeFactory;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDFTerm;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;

public class StatementStoreImpl implements StatementStore {

    private final KeyValueStore keyValueStore;

    public StatementStoreImpl(KeyValueStore keyValueStore) {
        this.keyValueStore = keyValueStore;
    }

    @Override
    public void put(Pair<RDFTerm, RDFTerm> queryKey, RDFTerm value) throws IOException {
        // write-once, read-many
        IRI key = calculateKeyFor(queryKey);
        String strValue = value instanceof IRI ? ((IRI) value).getIRIString() : value.toString();
        keyValueStore.put(key, IOUtils.toInputStream(strValue, StandardCharsets.UTF_8));
    }

    protected static IRI calculateKeyFor(Pair<RDFTerm, RDFTerm> unhashedKeyPair) {
        IRI left = calculateHashFor(unhashedKeyPair.getLeft());
        IRI right = calculateHashFor(unhashedKeyPair.getRight());
        return Hasher.calcSHA256(left.getIRIString() + right.getIRIString());
    }

    protected static IRI calculateHashFor(RDFTerm left1) {
        return Hasher.calcSHA256(RDFUtil.getValueFor(left1));
    }

    @Override
    public IRI get(Pair<RDFTerm, RDFTerm> queryKey) throws IOException {
        try(InputStream inputStream = keyValueStore.get(calculateKeyFor(queryKey))) {
            return inputStream == null
                    ? null
                    : RefNodeFactory.toIRI(URI.create(IOUtils.toString(inputStream, StandardCharsets.UTF_8)));
        }
    }
}
