package bio.guoda.preston.store;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;

/**
 * retrieves and validates query results
 * and puts only validated results into validated blobstore
 */

public class KeyValueStoreWithValidation implements KeyValueStore {


    private final ValidatingKeyValueStreamFactory validatingKeyValueStreamFactory;
    private final KeyValueStore validated;
    private final KeyValueStoreWithRemove staging;
    private final KeyValueStoreReadOnly backing;

    public KeyValueStoreWithValidation(
            ValidatingKeyValueStreamFactory validatingKeyValueStreamFactoryValues,
            KeyValueStoreWithRemove staging,
            KeyValueStore validated,
            KeyValueStoreReadOnly backing
    ) {
        this.validatingKeyValueStreamFactory = validatingKeyValueStreamFactoryValues;
        this.staging = staging;
        this.validated = validated;
        this.backing = backing;
    }

    @Override
    public IRI put(KeyGeneratingStream keyGeneratingStream, InputStream is) throws IOException {
        return validated.put(keyGeneratingStream, is);
    }

    @Override
    public void put(IRI key, InputStream is) throws IOException {
        try {
            ValidatingKeyValueStream keyValueStream = validatingKeyValueStreamFactory.forKeyValueStream(key, is);
            staging.put(key, keyValueStream.getValueStream());
            validate(key, keyValueStream);
        } finally {
            staging.remove(key);
        }
    }

    private void validate(IRI key, ValidatingKeyValueStream keyValueStream) throws IOException {
        if (keyValueStream.acceptValueStreamForKey(key)) {
            validated.put(key, staging.get(key));
        } else {
            throw new IOException("invalid results received for query [" + key.getIRIString() + "] because [" + StringUtils.join(keyValueStream.getViolations(), ", and because ") + "]");
        }
    }

    @Override
    public InputStream get(IRI key) throws IOException {
        InputStream inputStreamUnverified = backing.get(key);
        InputStream inputStreamVerified = null;
        if (inputStreamUnverified != null) {
            try {
                ValidatingKeyValueStream keyValueStream = validatingKeyValueStreamFactory.forKeyValueStream(key, inputStreamUnverified);
                staging.put(key, keyValueStream.getValueStream());
                validate(key, keyValueStream);
                inputStreamVerified = validated.get(key);
            } finally {
                staging.remove(key);
            }
        }
        return inputStreamVerified;
    }
}
