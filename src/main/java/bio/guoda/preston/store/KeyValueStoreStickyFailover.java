package bio.guoda.preston.store;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public class KeyValueStoreStickyFailover implements KeyValueStoreReadOnly {

    private final List<KeyValueStoreReadOnly> keyStoreCandidates;

    private AtomicReference<KeyValueStoreReadOnly> lastSuccessful = new AtomicReference<>();

    public KeyValueStoreStickyFailover(List<KeyValueStoreReadOnly> keyStoreCandidates) {
        this.keyStoreCandidates = keyStoreCandidates;
    }

    @Override
    public InputStream get(String key) throws IOException {
        AtomicReference<Exception> lastException = new AtomicReference<>();
        // try last successful first
        try {
            if (lastSuccessful.get() != null) {
                InputStream inputStream = lastSuccessful.get().get(key);
                if (inputStream != null) {
                    return inputStream;
                }
            }
        } catch (IOException ex) {
            lastException.set(ex);
        }

        for (KeyValueStoreReadOnly keyStoreCandidate : keyStoreCandidates) {
            if (lastSuccessful.get() == null
                    || (lastSuccessful.get() != null && lastSuccessful.get() != keyStoreCandidate)) {
                try {
                    InputStream inputStream = keyStoreCandidate.get(key);
                    if (inputStream == null) {
                        lastException.set(null);
                    } else {
                        lastSuccessful.set(keyStoreCandidate);
                        return inputStream;
                    }
                } catch (IOException ex) {
                    // ignore
                    lastException.set(ex);
                }
            }
        }

        if (lastException.get() != null) {
            lastSuccessful.set(null);
            throw new IOException("failed to retrieve [" + key + "]", lastException.get());
        } else {
            return null;
        }
    }


}
