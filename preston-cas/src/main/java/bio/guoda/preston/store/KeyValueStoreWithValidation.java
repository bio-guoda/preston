package bio.guoda.preston.store;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;

/**
 * retrieves and validates query results
 * and puts only validated results into verified blobstore
 */

public class KeyValueStoreWithValidation implements KeyValueStore {


    private final KeyValueStreamFactory keyValueStreamFactory;
    private final KeyValueStore verified;
    private final KeyValueStore staging;
    private final KeyValueStore backing;

    public KeyValueStoreWithValidation(
            KeyValueStreamFactory keyValueStreamFactoryValues,
            KeyValueStore staging,
            KeyValueStore verified,
            KeyValueStore backing
    ) {
        this.keyValueStreamFactory = keyValueStreamFactoryValues;
        this.staging = staging;
        this.verified = verified;
        this.backing = backing;
    }

    @Override
    public IRI put(KeyGeneratingStream keyGeneratingStream, InputStream is) throws IOException {
        throw new IOException("not implemented");
    }

    @Override
    public void put(IRI key, InputStream is) throws IOException {
        ValidatingKeyValueStream keyValueStream = keyValueStreamFactory.forKeyValueStream(key, is);
        staging.put(key, keyValueStream.getValueStream());
        validate(key, keyValueStream);
    }

    void validate(IRI key, ValidatingKeyValueStream keyValueStream) throws IOException {
        if (keyValueStream.acceptValueStreamForKey(key)) {
            verified.put(key, staging.get(key));
        } else {
            throw new IOException("invalid results received for query [" + key.getIRIString() + "] because [" + StringUtils.join(keyValueStream.getViolations(), ", and because ") + "]");
        }
    }

    @Override
    public InputStream get(IRI key) throws IOException {
        InputStream inputStreamUnverified = backing.get(key);
        InputStream inputStreamVerified = null;
        if (inputStreamUnverified != null) {
            ValidatingKeyValueStream keyValueStream = keyValueStreamFactory.forKeyValueStream(key, inputStreamUnverified);
            staging.put(key, keyValueStream.getValueStream());
            validate(key, keyValueStream);
            inputStreamVerified = verified.get(key);
        }
        return inputStreamVerified;
    }
}
