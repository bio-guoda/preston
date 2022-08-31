package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import bio.guoda.preston.Hasher;
import bio.guoda.preston.RDFValueUtil;
import bio.guoda.preston.RefNodeFactory;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDFTerm;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;

public class HexaStoreImpl implements HexaStore {

    private final QueryKeyCalculator queryKeyCalculator;
    private final KeyValueStore keyValueStore;

    public HexaStoreImpl(KeyValueStore keyValueStore, HashType type) {
        this.keyValueStore = keyValueStore;
        this.queryKeyCalculator = new QueryKeyCalculatorBackwardCompatible(type);
    }

    @Override
    public void put(Pair<RDFTerm, RDFTerm> queryKey, RDFTerm value) throws IOException {
        // write-once, read-many
        IRI key = queryKeyCalculator.calculateKeyFor(queryKey);
        String strValue = value instanceof IRI ? ((IRI) value).getIRIString() : value.toString();

        if (StringUtils.isBlank(strValue)
                || !HashKeyUtil.isValidHashKey(RefNodeFactory.toIRI(strValue), HashKeyUtil.hashTypeFor(key))) {
            throw new IOException("cannot store " + (StringUtils.isBlank(strValue) ? "empty value" : "malformed value [" + strValue + "]"));
        }

        if (StringUtils.isNotBlank(strValue)) {
            keyValueStore.put(key, IOUtils.toInputStream(strValue, StandardCharsets.UTF_8));
        }
    }


    protected static IRI calculateHashFor(RDFTerm term, HashType type) {
        return Hasher.calcHashIRI(RDFValueUtil.getValueFor(term), type);
    }

    @Override
    public IRI get(Pair<RDFTerm, RDFTerm> queryKey) throws IOException {
        IRI keyIRI = queryKeyCalculator.calculateKeyFor(queryKey);
        IRI valueIRI = null;
        try (InputStream inputStream = keyValueStore.get(keyIRI)) {
            if (inputStream != null) {
                valueIRI = RefNodeFactory.toIRI(URI.create(IOUtils.toString(inputStream, StandardCharsets.UTF_8)));
            }
        }
        if (valueIRI == null || !HashKeyUtil.isValidHashKey(valueIRI, HashKeyUtil.hashTypeFor(keyIRI))) {
            throw new IOException("found " + (valueIRI == null ? "empty" : "malformed query result [" + valueIRI.getIRIString() + "]"));
        }
        return valueIRI;
    }
}
