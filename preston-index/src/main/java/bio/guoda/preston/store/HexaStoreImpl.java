package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import bio.guoda.preston.Hasher;
import bio.guoda.preston.RDFValueUtil;
import bio.guoda.preston.RefNodeFactory;
import org.apache.commons.io.IOUtils;
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
    private final HashType type;

    public HexaStoreImpl(KeyValueStore keyValueStore, HashType type) {
        this.keyValueStore = keyValueStore;
        this.queryKeyCalculator = new QueryKeyCalculatorBackwardCompatible(type);
        this.type = type;
    }

    @Override
    public void put(Pair<RDFTerm, RDFTerm> queryKey, RDFTerm value) throws IOException {
        // write-once, read-many
        IRI key = queryKeyCalculator.calculateKeyFor(queryKey);
        String strValue = value instanceof IRI
                ? ((IRI) value).getIRIString()
                : value.toString();

        if (!type.equals(HashKeyUtil.hashTypeFor(strValue))) {
            throw new IOException("failed to write query result IRI: expected hash iri matching [" + type.getIRIPatternString() + "], but found [" + strValue + "] instead.");
        }

        keyValueStore.put(key, IOUtils.toInputStream(strValue, StandardCharsets.UTF_8));
    }


    static IRI calculateHashFor(RDFTerm term, HashType type) {
        return Hasher.calcHashIRI(RDFValueUtil.getValueFor(term), type);
    }

    @Override
    public IRI get(Pair<RDFTerm, RDFTerm> queryKey) throws IOException {
        IRI keyCalculated = queryKeyCalculator.calculateKeyFor(queryKey);
        try (InputStream inputStream = keyValueStore.get(keyCalculated)) {
            return getSupportedHashIRIOrThrow(keyCalculated, inputStream);
        }

    }

    private IRI getSupportedHashIRIOrThrow(IRI keyCalculated, InputStream inputStream) throws IOException {
        IRI iri;
        if (inputStream == null) {
            throw new IOException("failed to retrieve results for query key [" + keyCalculated.getIRIString() + "]: no data provided");
        } else {
            ValidatingKeyValueStream validatingKeyValueStream = new ValidatingKeyValueStreamHashTypeIRIFactory(type).forKeyValueStream(keyCalculated, inputStream);
            InputStream valueStream = validatingKeyValueStream.getValueStream();
            IRI iriFound = RefNodeFactory.toIRI(URI.create(IOUtils.toString(valueStream, StandardCharsets.UTF_8)));
            iri = validatingKeyValueStream.acceptValueStreamForKey(iriFound)
                    ? iriFound
                    : null;
            if (iri == null) {
                if (iriFound == null) {
                    throw new IOException("failed to retrieve results for query key [" + keyCalculated.getIRIString() + "]: no results found");
                } else {
                    throw new IOException("failed to retrieve results for query key [" + keyCalculated.getIRIString() + "]: invalid result key [" + iriFound.getIRIString() + "]");
                }
            }
        }

        return iri;
    }
}
